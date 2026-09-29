from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("p.order_id"),
        F.col("p.method"),
        F.col("o.customer_id"),
        F.col("o.order_date"),
        F.col("o.discount"),
        F.upper(F.col("o.status")).alias("status_upper"),
    )
)
