from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.customer_id"),
        F.col("o.status"),
        F.col("c.segment"),
        F.col("c.age"),
        F.col("c.name"),
        F.when(F.col("c.is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)
