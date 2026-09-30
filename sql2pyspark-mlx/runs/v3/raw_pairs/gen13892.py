from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.upper(F.col("c.name")).like("%IA %"))
    .groupBy(F.col("c.segment"))
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set(F.col("s.browser"))).alias("browser_list"),
    )
)
