from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") <= "2024-08-01"), "left_anti")
)

kept = (
    eligible
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("c.tags"), "premium")), "left_semi")
)

with_last = (
    kept
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.supplier_name", "s.country", "s.city", F.col("c.category"))
)

cleaned = (
    with_last
    .filter(F.col("category").isNotNull())
    .select(
        "id",
        "supplier_name",
        "category",
        F.length("city").alias("city_length"),
        F.substring("category", 1, 2).alias("category_prefix"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.city_length", "s.category_prefix", F.col("c.price"))
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("city_length") >= 83, "PREMIUM").otherwise("STANDARD").alias("city_length_tier"),
        F.when(F.col("price") >= 1270, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.percentile_approx("price", 0.5).alias("price_p50"),
        F.percentile_approx("city_length", 0.5).alias("city_length_p50"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("distinct_ids")
)
