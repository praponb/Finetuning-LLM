from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.filter(F.col("age") < 45).alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "left_semi")
    .select(F.col("orders.id"), F.col("orders.discount"))
    .orderBy("discount")
)
