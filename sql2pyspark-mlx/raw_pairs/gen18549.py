from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.filter(F.col("store_id").isin([7, 8, 21, 29])).alias("orders"), F.col("customers.id") == F.col("orders.customer_id"), "left_anti")
    .select(F.col("customers.id"), F.col("customers.is_active"))
    .orderBy(F.col("id").desc())
)
