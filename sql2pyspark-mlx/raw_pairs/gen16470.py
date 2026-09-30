from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_sessions.filter(F.col("customer_id").isin([7, 27])).alias("sessions"), F.col("customers.id") == F.col("sessions.customer_id"), "left_semi")
    .select(
        F.col("customers.id"),
        F.col("customers.segment"),
        F.col("customers.signup_date"),
        F.col("customers.age"),
    )
)
