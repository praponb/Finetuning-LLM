from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.channel").isin(["web", "store", "mobile"]))
    .select(
        F.col("p.method"),
        F.col("p.amount"),
        F.col("o.customer_id"),
        F.col("o.status"),
        F.col("o.channel"),
    )
)
