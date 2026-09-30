from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_sessions.alias("sessions"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("sessions.customer_id").isNull())
    .select(
        F.col("customers.credit_limit"),
        F.col("sessions.country"),
        F.when(F.col("customers.country") == "TH", 1).otherwise(0).alias("is_th"),
    )
)
