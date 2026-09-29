from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .filter(F.col("sessions.customer_id").isNotNull())
    .join(df_customers.filter(F.col("city").isin(["Bangkok", "Berlin"])).alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left_anti")
    .select(F.col("sessions.id"), F.col("sessions.browser"))
)
