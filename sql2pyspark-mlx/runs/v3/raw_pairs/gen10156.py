from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "outer")
    .filter(F.year(F.col("customers.signup_date")) == 2023)
    .select(
        F.col("sessions.id"),
        F.col("sessions.browser"),
        F.col("sessions.country"),
        F.col("customers.age"),
        F.col("customers.segment"),
    )
    .orderBy("segment")
)
