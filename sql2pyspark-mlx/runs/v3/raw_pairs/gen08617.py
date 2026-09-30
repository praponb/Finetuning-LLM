from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("customers.segment").isNull() & F.col("sessions.customer_id").isNull())
    .select(
        F.col("sessions.id"),
        F.col("sessions.country"),
        F.col("sessions.device"),
        F.col("customers.age"),
        F.col("customers.credit_limit"),
    )
)
