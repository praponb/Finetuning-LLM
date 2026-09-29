from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("country").orderBy("price", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_suppliers
    .filter(F.col("city").eqNullSafe(F.lit("London")))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.concat("city", F.lit("/"), "country").alias("city_country"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("ch.tags"), "premium")), "left_semi")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category") == "Books"), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        F.col("c.category").alias("product_category"),
        F.col("c.price"),
    )
)

valid = (
    latest_match
    .filter(F.col("product_category").isNotNull())
    .withColumn("is_th", F.when(F.col("country") == "TH", 1).otherwise(0))
    .withColumn("product_category_filled", F.coalesce("product_category", F.lit("UNKNOWN")))
)

metrics = (
    valid
    .withColumn("last3_count_price", F.count("price").over(w_hist))
    .withColumn("last3_sum_price", F.sum("price").over(w_hist))
    .withColumn("last3_avg_price", F.avg("price").over(w_hist))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("last3_sum_price") >= 4800, "LARGE").otherwise("SMALL").alias("last3_sum_price_tier"),
        F.when(F.col("price") >= 1100, "HIGH").otherwise("LOW").alias("price_tier"),
    )
    .agg(
        F.percentile_approx("last3_sum_price", [0.25, 0.5, 0.75]).alias("last3_sum_price_quartiles"),
        F.max("is_th").alias("max_is_th"),
        F.count_if(F.col("product_category") == "Books").alias("books_events"),
    )
    .orderBy("price_tier")
)
