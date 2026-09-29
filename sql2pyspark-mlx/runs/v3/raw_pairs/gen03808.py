from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("s.shipped_date"),
        F.col("s.status"),
        F.col("o.order_date"),
        F.col("c.country"),
        F.col("c.signup_date"),
        F.when(F.col("c.is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)
