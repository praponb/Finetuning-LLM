from pyspark.sql import functions as F

joined = (
    df_store_sales
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.units",
        F.col("b.brand"),
        F.col("b.cost"),
    )
)

result = (
    joined
    .withColumn("units_tier", F.when(F.col("units") >= 35, "PREMIUM").otherwise("STANDARD"))
    .rollup("units_tier")
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_rows"),
        F.bit_and("units").alias("units_bit_and"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("acme_rows").desc())
)
