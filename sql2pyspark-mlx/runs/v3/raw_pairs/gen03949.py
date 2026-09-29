from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .select(F.col("sessions.started_at"), F.col("customers.credit_limit"))
    .orderBy(F.col("started_at").desc())
)
