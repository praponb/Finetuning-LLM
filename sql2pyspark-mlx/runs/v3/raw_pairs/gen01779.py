from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.browser") == "Firefox")
    .select(F.col("s.country"), F.col("s.browser"), F.col("c.age"))
    .orderBy("browser")
)
