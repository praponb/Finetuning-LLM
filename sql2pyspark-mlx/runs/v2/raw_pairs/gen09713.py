from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.country").isNotNull())
    .groupBy(F.col("s.customer_id"))
    .agg(
        F.sort_array(F.collect_set(F.col("we.page"))).alias("page_list"),
        F.countDistinct(F.col("we.duration_sec")).alias("unique_sessions"),
    )
    .filter(F.col("unique_sessions") > 3)
    .orderBy(F.col("customer_id").desc())
)
