from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "inner")
    .filter((F.year(F.col("orders.order_date")) == 2023) & F.col("orders.amount").between(420, 810))
    .select(
        F.col("payments.id"),
        F.col("payments.method"),
        F.col("payments.amount"),
        F.col("orders.order_date"),
        F.col("orders.discount"),
        F.col("orders.status"),
        F.date_trunc("month", F.col("orders.order_date")).alias("order_date_period"),
    )
)
