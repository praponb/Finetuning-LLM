from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.is_active") | F.col("customers.city").isin(["New York", "Bangkok"]))
    .select(
        F.col("sessions.started_at"),
        F.col("sessions.browser"),
        F.col("sessions.device"),
        F.col("customers.city"),
        F.col("customers.segment"),
        F.col("customers.signup_date"),
    )
    .orderBy("city")
)
