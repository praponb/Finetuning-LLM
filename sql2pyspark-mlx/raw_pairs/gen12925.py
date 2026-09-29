from pyspark.sql import functions as F

kept_keys = (
    df_inventory
    .select("product_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("product_id") == 2)
        .select("product_id", "id")
    )
)

reconciled = (
    df_inventory
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("quantity") >= 204, "HIGH").otherwise("LOW").alias("quantity_tier"),
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.count("*").alias("cnt"),
        F.max("quantity").alias("max_quantity"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.sum("quantity").alias("total_quantity"),
    )
    .orderBy(F.col("max_quantity").desc_nulls_last())
)
