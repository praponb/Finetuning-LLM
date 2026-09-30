from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.discount") >= 0.2)
    .select(F.col("o.amount"), F.col("c.name"), F.col("c.city"), F.col("c.age"))
    .orderBy(F.col("age").desc())
)
