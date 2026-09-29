from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.name").like("%smi%"))
    .groupBy(F.col("c.country"))
    .agg(F.min(F.col("s.start_date")).alias("min_start_date"))
    .orderBy(F.col("min_start_date").desc())
)
