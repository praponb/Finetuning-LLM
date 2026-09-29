from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .filter(F.col("c.city") == "London")
    .select(F.col("c.id"), F.col("c.signup_date"))
    .orderBy(F.col("id").desc())
)
