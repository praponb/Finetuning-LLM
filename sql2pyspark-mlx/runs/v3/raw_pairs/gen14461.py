from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("sessions.country").isin(["SG", "JP"]))
    .groupBy(
        F.when(F.col("customers.age") >= 59, "high").when(F.col("customers.age") >= 37, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.count(F.when(F.col("sessions.browser") == "Safari", 1)).alias("safari_count"),
    )
    .orderBy(F.col("safari_count").desc())
)
