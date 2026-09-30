from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "outer")
    .select(
        F.col("sessions.id"),
        F.col("sessions.browser"),
        F.col("sessions.country"),
        F.col("customers.country").alias("customer_country"),
        F.col("customers.city"),
        F.col("customers.age"),
    )
    .orderBy("browser", "age")
)
