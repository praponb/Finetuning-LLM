from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("monthly_fee") <= 190).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .select(F.col("c.id"), F.col("c.city"), F.col("c.segment"), F.col("c.name"))
    .orderBy("id", F.col("segment").desc())
)
