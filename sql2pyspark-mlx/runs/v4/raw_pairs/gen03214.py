from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_sessions.alias("sessions"), F.col("customers.id") == F.col("sessions.customer_id"), "left")
    .filter(F.col("sessions.browser").isin(["Chrome", "Edge"]))
    .groupBy(F.col("customers.id"), F.col("customers.email"))
    .agg(F.count(F.col("sessions.id")).alias("session_count"))
    .filter(F.col("session_count") >= 1)
    .orderBy(F.col("session_count").desc())
)
