from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .groupBy(
        F.when(F.col("customers.age") >= 59, "high").when(F.col("customers.age") >= 39, "mid").otherwise("low").alias("age_bucket"),
        F.date_format(F.col("customers.signup_date"), "yyyy-MM").alias("signup_month"),
    )
    .agg(F.countDistinct(F.col("sessions.device")).alias("unique_devices"))
    .filter(F.col("unique_devices") > 4)
    .orderBy("unique_devices", "age_bucket", "signup_month")
    .limit(10)
)
