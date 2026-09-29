from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.region"))
    .agg(
        F.count(F.col("o.id")).alias("order_count"),
        (F.max(F.col("o.amount")) - F.min(F.col("o.amount"))).alias("amount_range"),
        F.sum(F.when(F.col("o.status") == "PENDING", 1).otherwise(0)).alias("pending_count"),
    )
)
