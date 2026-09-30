from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(F.length("name") > 11).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.monthly_fee").between(90, 230))
    .select(F.col("s.id"), F.col("s.status"), F.col("s.plan"), F.col("s.customer_id"))
)
