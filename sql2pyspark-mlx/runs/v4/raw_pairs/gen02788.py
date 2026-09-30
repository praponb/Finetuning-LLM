from pyspark.sql import functions as F

matched_keys = (
    df_inventory
    .select("store_id", "id")
    .exceptAll(
        df_inventory
        .filter(F.col("product_id").isin([2, 3, 6, 9]))
        .select("store_id", "id")
    )
)

verified = (
    df_inventory
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("activity_month"))
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.sum("quantity").alias("total_quantity"),
        F.avg("quantity").alias("avg_quantity"),
    )
    .orderBy("avg_quantity")
)
