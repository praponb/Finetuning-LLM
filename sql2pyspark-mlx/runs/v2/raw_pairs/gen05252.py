from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "left")
    .filter(F.col("sessions.browser").isNotNull())
    .groupBy(
        F.when(F.col("customers.age") >= 57, "high").when(F.col("customers.age") >= 35, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.sort_array(F.collect_set(F.col("sessions.device"))).alias("device_list"))
    .orderBy(F.col("age_bucket").desc())
)
