from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.col("customers.country"))
    .agg(F.countDistinct(F.col("sessions.customer_id")).alias("unique_customers"))
    .orderBy(F.col("unique_customers").desc(), "country")
)
