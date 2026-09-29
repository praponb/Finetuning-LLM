from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter((F.col("p.paid_at") < "2024-04-01") & F.col("p.amount").between(520, 1380))
    .select(
        F.col("p.amount"),
        F.col("p.order_id"),
        F.col("o.channel"),
        F.col("o.order_date"),
        F.col("o.status"),
    )
)
