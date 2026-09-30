from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.segment").isin(["SMB", "Corporate"]))
    .select(F.col("o.channel"), F.col("c.city"), F.col("c.is_active"))
    .orderBy("channel")
)
