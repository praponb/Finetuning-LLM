from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter(F.col("c.age") >= 56)
    .select(
        F.col("s.started_at"),
        F.col("c.country"),
        (F.col("c.age") * F.col("c.credit_limit")).alias("age_times_credit_limit"),
    )
    .orderBy("country")
)
