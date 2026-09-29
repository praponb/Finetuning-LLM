from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("o.status"))
    .agg(
        F.sum(F.when(F.col("p.status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
        (F.max(F.col("p.amount")) - F.min(F.col("p.amount"))).alias("amount_range"),
    )
)
