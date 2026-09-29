from pyspark.sql import functions as F

remaining_keys = (
    df_inventory
    .select("product_id")
    .exceptAll(
        df_inventory
        .filter(F.col("store_id") == 3)
        .select("product_id")
    )
)

reconciled = (
    df_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("MONTH", "last_restock").alias("activity_month"))
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("quantity_bit_and") > 1)
)
