from pyspark.sql import functions as F

excluded_customers = df_customers.filter(F.col("country") == "JP").select("id")

result = (
    df_sessions.alias("sessions")
    .join(excluded_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left_anti")
    .filter(F.col("sessions.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .select(
        F.col("sessions.id"),
        F.col("sessions.country"),
        F.col("sessions.device"),
        F.col("sessions.customer_id"),
    )
)
