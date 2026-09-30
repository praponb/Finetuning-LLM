from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.filter(F.col("age") >= 54).alias("c"), F.col("o.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("o.channel") == "store")
    .select(F.col("o.id"), F.col("o.store_id"))
)
