from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_sessions.alias("sessions"), F.col("customers.id") == F.col("sessions.customer_id"), "inner")
    .select(
        F.col("customers.city"),
        F.col("customers.name"),
        F.col("sessions.customer_id"),
        F.col("sessions.country"),
    )
    .orderBy("country")
)
