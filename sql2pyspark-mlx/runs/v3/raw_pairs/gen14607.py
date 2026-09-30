from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "right")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("c.email").like("%@GMAIL.COM"))
    .select(
        F.col("o.shipping_fee"),
        F.col("o.customer_id"),
        F.col("c.name"),
        F.col("s.store_name"),
        F.when(F.col("c.is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
    .orderBy("store_name")
)
