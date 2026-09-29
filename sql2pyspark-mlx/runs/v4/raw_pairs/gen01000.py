from pyspark.sql import functions as F

excluded_customers = df_customers.filter(~F.col("segment").isin(["Consumer", "Corporate", "SMB"])).select("id")

result = (
    df_sessions.alias("sessions")
    .join(excluded_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left_anti")
    .filter(F.col("sessions.customer_id").isNotNull() | F.lit(excluded_customers.isEmpty()))
    .filter(F.col("sessions.device") == "tablet")
    .select(
        F.col("sessions.id"),
        F.col("sessions.customer_id"),
        F.col("sessions.started_at"),
        F.col("sessions.device"),
    )
)
