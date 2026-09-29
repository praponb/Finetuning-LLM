from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("customers.email").like("%@outlook.com"))
    .groupBy(
        F.when(F.col("customers.country").isin(["US", "DE"]), "tier_1").otherwise("other").alias("country_group"),
    )
    .agg(F.max(F.col("sessions.started_at")).alias("last_started_at"))
    .orderBy(F.col("country_group").desc())
)
