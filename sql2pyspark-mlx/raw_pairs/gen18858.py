from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "inner")
    .filter(~(~F.col("customers.is_active") | F.col("sessions.customer_id").isin([33, 34])))
    .select(
        F.col("sessions.id"),
        F.col("sessions.customer_id"),
        F.col("sessions.browser"),
        F.col("customers.signup_date"),
    )
    .orderBy("signup_date")
)
