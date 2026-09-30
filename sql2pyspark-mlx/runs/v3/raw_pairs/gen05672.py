from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.month(F.col("c.signup_date")).isin([2, 7, 10]))
    .groupBy(F.col("c.id"), F.col("c.country"))
    .agg(
        F.count(F.col("s.id")).alias("session_count"),
        F.countDistinct(F.col("s.browser")).alias("unique_browsers"),
        F.sort_array(F.collect_set(F.col("s.device"))).alias("device_list"),
    )
    .orderBy("id", "country")
    .limit(20)
)
