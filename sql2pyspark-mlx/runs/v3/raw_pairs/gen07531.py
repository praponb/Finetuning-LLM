from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.filter(F.col("age") < 43).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .select(F.col("s.id"), F.col("s.customer_id"), F.col("s.start_date"))
    .orderBy(F.col("customer_id").desc())
)
