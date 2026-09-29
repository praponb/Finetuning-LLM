from pyspark.sql import functions as F

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= "2023-09-01")
    .withColumn("quantity_rounded", F.round("quantity", 2))
)

result = (
    cleaned
    .rollup("store_id")
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("quantity_rounded", 0.9).alias("quantity_rounded_p90"),
        F.min("quantity").alias("min_quantity"),
        F.count("*").alias("num_rows"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("quantity_rounded_p90").desc_nulls_first())
)
