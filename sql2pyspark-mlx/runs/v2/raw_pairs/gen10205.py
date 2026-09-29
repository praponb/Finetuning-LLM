from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_orders.alias("orders"), F.col("customers.id") == F.col("orders.customer_id"), "left_semi")
    .select(F.col("customers.id"), F.col("customers.segment"))
)
