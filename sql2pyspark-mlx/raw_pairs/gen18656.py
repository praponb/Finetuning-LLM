from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.store_id").isNotNull())
    .select(F.col("c.age"), F.col("o.discount"))
)
