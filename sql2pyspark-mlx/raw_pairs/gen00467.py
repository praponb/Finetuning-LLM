from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .filter(~F.col("p.method").isin(["paypal", "cash", "card"]))
    .select(
        F.col("p.status"),
        F.col("o.amount"),
        F.col("o.discount"),
        F.col("o.channel"),
    )
)
