from pyspark.sql import functions as F

store_summary = (
    df_orders
    .filter(F.col("amount") >= 460)
    .groupBy("store_id")
    .agg(
        F.count("*").alias("num_orders"),
        F.count(F.when(F.col("status") == "SHIPPED", 1)).alias("shipped_count"),
    )
)

result = (
    df_stores.alias("s")
    .join(store_summary.alias("ss"), F.col("s.id") == F.col("ss.store_id"), "inner")
    .select(
        F.col("s.id"),
        F.col("s.opened_date"),
        F.col("ss.num_orders"),
        F.col("ss.shipped_count"),
    )
)
