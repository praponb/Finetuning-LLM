from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.city").isin(["Singapore", "New York"]))
    .select(F.col("o.id"), F.col("o.status"), F.col("c.name"))
)
