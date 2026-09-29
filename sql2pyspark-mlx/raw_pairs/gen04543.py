from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "left")
    .select(
        F.col("o.order_date"),
        F.col("o.amount"),
        F.col("s.store_name"),
        F.col("s.opened_date"),
        F.col("c.segment"),
        F.col("c.age"),
        F.when(F.col("s.region").isin(["Central", "East"]), "Priority").otherwise("Other").alias("region_group"),
    )
)
