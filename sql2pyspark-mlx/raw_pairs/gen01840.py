from pyspark.sql import functions as F

kept_keys = (
    df_inventory
    .select("store_id", "id")
    .exceptAll(
        df_inventory
        .filter(F.col("quantity").between(102, 238))
        .select("store_id", "id")
    )
)

verified = (
    df_inventory
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 242, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.count("*").alias("row_count"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.min("quantity").alias("min_quantity"),
    )
    .orderBy(F.col("row_count").desc_nulls_last())
)
