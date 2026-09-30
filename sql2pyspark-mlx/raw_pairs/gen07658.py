from pyspark.sql import functions as F

remaining_keys = (
    df_inventory
    .select("store_id")
    .intersectAll(
        df_inventory
        .filter(F.col("last_restock").between("2024-01-01", "2024-10-02"))
        .select("store_id")
    )
)

verified = (
    df_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("product_id")
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
    )
    .orderBy(F.col("quantity_bit_and").asc_nulls_first())
)
