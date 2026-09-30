from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_sessions.filter(F.col("browser").isin(["Chrome", "Firefox", "Edge"])).alias("sessions"), F.col("customers.id") == F.col("sessions.customer_id"), "left_anti")
    .select(
        F.col("customers.id"),
        F.col("customers.is_active"),
        F.col("customers.signup_date"),
        F.col("customers.country"),
    )
)
