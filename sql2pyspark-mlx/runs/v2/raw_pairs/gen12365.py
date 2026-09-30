from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "right")
    .filter(F.month(F.col("c.signup_date")).isin([1, 6, 7]))
    .groupBy(F.col("c.segment"))
    .agg(F.countDistinct(F.col("s.device")).alias("unique_devices"))
    .filter(F.col("unique_devices") >= 4)
    .orderBy("unique_devices", "segment")
)
