from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "right")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("o.customer_id").isNotNull())
    .select(F.col("o.store_id"), F.col("c.segment"), F.col("c.email"), F.col("s.sqft"))
    .orderBy("email")
)
