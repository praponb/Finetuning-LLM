from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.device") == "mobile")
    .groupBy(F.col("c.segment"))
    .agg(
        F.sort_array(F.collect_set(F.col("s.country"))).alias("country_list"),
        F.countDistinct(F.col("s.browser")).alias("unique_browsers"),
    )
    .orderBy(F.col("unique_browsers").desc())
)
