from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "inner")
    .filter(F.col("c.city").isNotNull())
    .select(F.col("o.discount"), F.col("c.segment"))
)
