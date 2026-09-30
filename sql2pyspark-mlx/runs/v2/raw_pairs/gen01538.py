from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.age") < 48)
    .groupBy(F.col("s.customer_id"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("we.page") == "/home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
        F.round(((100 * F.sum(F.when(F.col("we.page") == "/checkout", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checkout"),
    )
    .orderBy(F.col("pct_home").desc(), "customer_id")
    .limit(5)
)
