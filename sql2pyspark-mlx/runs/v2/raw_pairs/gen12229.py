from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("c.name").like("%Srisuk%"))
    .groupBy(F.col("c.city"))
    .agg(F.count(F.when(F.col("s.device") == "desktop", 1)).alias("desktop_count"))
    .orderBy(F.col("desktop_count").desc(), "city")
    .limit(1)
)
