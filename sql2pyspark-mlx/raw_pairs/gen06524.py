from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.filter(F.col("browser").isin(["Safari", "Firefox", "Chrome"])).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .filter(F.lower(F.col("c.email")).like("%vy@%"))
    .select(F.col("c.id"), F.col("c.signup_date"), F.col("c.country"))
)
