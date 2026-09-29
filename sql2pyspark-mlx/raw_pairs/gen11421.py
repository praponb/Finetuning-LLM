from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.filter(F.col("shipping_fee").between(27, 30)).alias("o"), F.col("c.id") == F.col("o.customer_id"), "left_semi")
    .select(F.col("c.id"), F.col("c.country"))
)
