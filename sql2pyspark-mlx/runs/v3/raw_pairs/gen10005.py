from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .groupBy(F.col("c.is_active"), F.col("c.segment"))
    .agg(
        F.sort_array(F.collect_set(F.col("s.browser"))).alias("browser_list"),
        F.countDistinct(F.col("s.country")).alias("unique_countrys"),
    )
)
