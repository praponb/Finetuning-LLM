from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .groupBy(
        F.when(F.col("customers.credit_limit") >= 17800, "high").when(F.col("customers.credit_limit") >= 4400, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("sessions.country")))).alias("country_values"),
    )
    .orderBy(F.col("credit_limit_bucket").desc())
)
