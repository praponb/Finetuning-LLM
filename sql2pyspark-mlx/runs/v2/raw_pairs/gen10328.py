from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.discount") <= 0.28)
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("p.amount"),
        F.col("o.store_id"),
        F.col("o.channel"),
    )
)
