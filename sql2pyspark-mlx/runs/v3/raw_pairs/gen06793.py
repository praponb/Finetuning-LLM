from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter((F.col("p.amount") > 860) & (F.col("o.discount") <= 0.14))
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("p.order_id"),
        F.col("p.amount"),
        F.col("o.order_date"),
        F.col("o.customer_id"),
        F.date_format(F.col("o.order_date"), "yyyy-MM-dd").alias("order_date_label"),
    )
)
