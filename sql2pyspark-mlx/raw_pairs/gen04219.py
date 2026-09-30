from pyspark.sql import functions as F

remaining_keys = (
    df_inventory
    .select("product_id", "store_id")
    .intersect(
        df_inventory
        .filter(F.col("quantity") != 218)
        .select("product_id", "store_id")
    )
)

verified = (
    df_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 194, "HIGH").otherwise("LOW"))
    .withColumn("activity_month", F.date_trunc("MONTH", "last_restock"))
    .rollup("quantity_tier", "activity_month")
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.grouping("quantity_tier").alias("gid"),
    )
    .orderBy(F.col("activity_month").desc())
)
