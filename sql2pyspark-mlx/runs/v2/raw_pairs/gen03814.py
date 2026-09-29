from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.filter(F.month("signup_date").isin([1, 4, 5])).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(F.col("s.device").isin(["tablet", "desktop", "mobile"]))
    .select(F.col("s.id"), F.col("s.device"), F.col("s.customer_id"))
    .orderBy(F.col("device").desc())
)
