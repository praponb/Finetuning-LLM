from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "inner")
    .filter(F.col("sessions.device").isin(["mobile", "desktop", "tablet"]) & (F.col("customers.signup_date") < "2025-09-01"))
    .select(
        F.col("sessions.id"),
        F.col("sessions.browser"),
        F.col("sessions.customer_id"),
        F.col("customers.email"),
        F.col("customers.city"),
        F.col("customers.name"),
    )
    .orderBy("city")
)
