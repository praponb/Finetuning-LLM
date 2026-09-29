from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.device").isin(["mobile", "desktop"]))
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("c.age"),
        F.col("c.signup_date"),
        F.col("c.city"),
        F.date_format(F.col("s.started_at"), "yyyy-MM").alias("started_at_label"),
    )
)
