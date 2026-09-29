from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.filter(F.col("browser").isNotNull()).alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_semi")
    .filter(F.col("c.name").like("FRA%"))
    .select(F.col("c.id"), F.col("c.name"))
    .orderBy(F.col("id").desc())
    .limit(5)
)
