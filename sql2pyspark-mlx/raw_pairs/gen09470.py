from pyspark.sql import functions as F

remaining_keys = (
    df_inventory
    .select("id", "store_id")
    .intersectAll(
        df_inventory
        .filter(F.col("store_id") == 1)
        .select("id", "store_id")
    )
)

kept_rows = (
    df_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("store_id")
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
    .orderBy(F.col("store_id").asc_nulls_first())
)
