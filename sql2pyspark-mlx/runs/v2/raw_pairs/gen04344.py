from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.year("end_date") >= 2025).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .filter(F.col("c.is_active"))
    .select(F.col("c.id"), F.col("c.segment"))
)
