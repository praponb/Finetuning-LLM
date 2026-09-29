from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("s.opened_date") >= "2024-02-01")
    .groupBy(F.col("s.city"), F.month(F.col("s.opened_date")).alias("opened_month"))
    .agg(
        F.sum(F.when(F.col("o.status") == "COMPLETED", F.col("o.amount")).otherwise(0)).alias("completed_amount"),
        F.sum(F.when(F.col("o.status") == "PENDING", 1).otherwise(0)).alias("pending_count"),
    )
)
