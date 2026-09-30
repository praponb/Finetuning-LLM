from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("s.browser") == "Firefox", 1).otherwise(0)).alias("firefox_count"),
        F.sort_array(F.collect_set(F.col("s.browser"))).alias("browser_list"),
    )
    .orderBy("signup_month", F.col("firefox_count").desc())
    .limit(10)
)
