from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "left")
    .filter((F.col("c.country") != "SG") & (F.col("c.country") == "UK"))
    .select(F.col("c.email"), F.col("c.is_active"), F.col("o.status"))
)
