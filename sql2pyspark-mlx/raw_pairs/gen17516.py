from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.col("signup_date") > "2024-12-01").select("id")

result = (
    df_sessions.alias("sessions")
    .join(excluded_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left_anti")
    .filter(F.col("sessions.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .filter(F.col("sessions.browser").isin(["Chrome", "Safari"]))
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("sessions.device"),
        F.col("sessions.country"),
    )
    .orderBy("country")
)
