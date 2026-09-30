from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.filter(F.year("order_date") >= 2025).alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_semi")
    .filter(F.col("c.name").isNotNull())
    .select(F.col("c.id"), F.col("c.age"))
)
