from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.filter(F.col("discount") < 0.16).alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_anti")
    .filter(F.col("c.age") >= 61)
    .select(F.col("c.id"), F.col("c.age"), F.col("c.is_active"))
    .orderBy(F.col("age").desc(), "id")
    .limit(5)
)
