from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.country") == "TH")
    .select(F.col("o.id"), F.col("o.amount"), F.col("c.country"))
)
