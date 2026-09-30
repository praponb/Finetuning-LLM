from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .filter(F.col("sessions.customer_id").isNotNull())
    .join(df_customers.filter(F.col("signup_date") <= "2023-08-17").alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left_anti")
    .filter(F.col("sessions.device").isin(["desktop", "mobile"]))
    .select(F.col("sessions.id"), F.col("sessions.device"), F.col("sessions.country"))
)
