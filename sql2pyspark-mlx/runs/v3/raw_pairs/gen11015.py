from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .filter(~F.col("sessions.country").isin(["SG", "JP", "US"]))
    .groupBy(F.col("customers.city"))
    .agg(
        F.count("*").alias("session_count"),
        F.count(F.when(F.col("sessions.device") == "mobile", 1)).alias("mobile_count"),
    )
)
