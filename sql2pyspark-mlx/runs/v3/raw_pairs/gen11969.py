from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "right")
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("c.name"),
        F.col("c.segment"),
        F.col("c.city"),
    )
    .orderBy(F.col("store_id").desc())
)
