from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_sessions.alias("sessions"), F.col("customers.id") == F.col("sessions.customer_id"), "left")
    .filter(F.col("customers.country").isin(["US", "DE", "UK"]))
    .select(
        F.col("customers.age"),
        F.col("customers.signup_date"),
        F.col("sessions.customer_id"),
    )
    .orderBy(F.col("signup_date").desc(), F.col("customer_id").desc())
)
