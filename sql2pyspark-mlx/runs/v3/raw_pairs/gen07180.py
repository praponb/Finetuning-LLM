from pyspark.sql import functions as F

store_stats = (
    df_orders
    .groupBy("store_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.count("*").alias("num_orders"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_stats.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "left")
    .select(F.col("s.store_name"), F.col("ss.min_amount"), F.col("ss.num_orders"))
    .orderBy("min_amount")
)
