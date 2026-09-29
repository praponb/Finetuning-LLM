from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.filter(F.col("shipping_fee") > 21).alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_anti")
    .select(F.col("c.id"), F.col("c.city"), F.col("c.age"))
    .orderBy(F.col("age").desc(), "city")
)
