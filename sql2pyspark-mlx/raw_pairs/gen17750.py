from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("s.country") == "DE")
    .groupBy(F.col("c.id"), F.col("c.country"))
    .agg(
        F.count(F.col("s.id")).alias("session_count"),
        F.sum(F.when(F.col("s.country") == "UK", 1).otherwise(0)).alias("uk_count"),
        F.sort_array(F.collect_set(F.col("s.country"))).alias("country_list"),
    )
    .filter(F.col("uk_count") > 70)
)
