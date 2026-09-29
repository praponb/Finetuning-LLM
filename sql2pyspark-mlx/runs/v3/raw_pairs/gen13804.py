from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(
        F.count("*").alias("total_count"),
        F.max(F.col("o.amount")).alias("max_amount"),
    )
)
