from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "left")
    .filter(F.col("sessions.browser").isNull())
    .groupBy(F.quarter(F.col("customers.signup_date")).alias("signup_quarter"))
    .agg(
        F.sum(F.when(F.col("sessions.browser") == "Edge", 1).otherwise(0)).alias("edge_count"),
        F.min(F.col("sessions.started_at")).alias("min_started_at"),
    )
)
