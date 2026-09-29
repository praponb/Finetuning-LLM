from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("o.store_id") == 23)
    .select(
        F.col("p.id"),
        F.col("p.method"),
        F.col("p.paid_at"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("o.order_date"),
        F.when(F.col("o.status").isin(["PENDING", "SHIPPED"]), "Priority").otherwise("Other").alias("status_group"),
    )
    .orderBy("order_date")
)
