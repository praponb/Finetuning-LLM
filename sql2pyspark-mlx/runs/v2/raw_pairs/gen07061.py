from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.filter(~F.col("country").isin(["DE", "US"])).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .filter(F.col("c.credit_limit") >= 13100)
    .select(
        F.col("c.id"),
        F.col("c.segment"),
        F.col("c.credit_limit"),
        F.col("c.email"),
    )
)
