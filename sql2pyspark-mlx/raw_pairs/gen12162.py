from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w2 = Window.orderBy(F.col("price").desc(), "id")
w3 = Window.partitionBy("city").orderBy("price", "id").rowsBetween(-6, -1)
w4 = Window.orderBy("price", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.price").desc(), F.col("c.id").desc())

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.length("supplier_name") > 7))
    .select(
        "id",
        "supplier_name",
        "city",
        F.concat("supplier_name", F.lit(" - "), "country").alias("supplier_name_country"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price"))
)

metrics = (
    with_latest
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "id",
        "city",
        F.ntile(3).over(w2).alias("price_bucket"),
        F.avg("price").over(w3).alias("price_avg_6_prior"),
        F.lag("price", 1).over(w4).alias("prev_price"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.stock_qty") > 307), "left_anti")
)

with_last = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.city", "s.price_bucket", "s.prev_price", F.col("c.cost"))
)

result = (
    with_last
    .groupBy(
        "city",
        F.when(F.col("price_bucket") >= 8, "LARGE").otherwise("SMALL").alias("price_bucket_tier"),
    )
    .agg(
        F.percentile_approx("prev_price", 0.95).alias("prev_price_p95"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("cost_per_london"),
        F.percentile_approx("prev_price", [0.25, 0.5, 0.75]).alias("prev_price_quartiles"),
        F.avg("prev_price").alias("avg_prev_price"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)
