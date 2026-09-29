from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.filter(F.col("customer_id").isin([4, 7, 8, 35])).alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_semi")
    .filter(F.col("c.age").isNull())
    .select(F.col("c.id"), F.col("c.email"), F.col("c.age"))
)
