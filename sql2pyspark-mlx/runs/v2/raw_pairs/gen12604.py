from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.month(F.col("c.signup_date")).isin([3, 11, 12]))
    .groupBy(F.col("s.browser"))
    .agg(
        F.sum(F.when(F.col("we.page") == "/home", 1).otherwise(0)).alias("home_count"),
        F.sort_array(F.collect_set(F.col("we.page"))).alias("page_list"),
    )
    .orderBy(F.col("home_count").desc(), "browser")
    .limit(50)
)
