from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_orders.alias("o"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.city") == "Bangkok")
    .select(F.col("c.city"), F.col("c.email"), F.col("o.discount"))
)
