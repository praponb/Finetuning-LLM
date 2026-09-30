from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.page").isin(["/checkout", "/cart"]))
    .groupBy(
        F.month(F.col("s.started_at")).alias("started_month"),
        F.when(F.col("s.browser").isin(["Edge", "Safari"]), "core").otherwise("other").alias("browser_group"),
    )
    .agg(
        F.round(F.avg(F.col("we.duration_sec")), 2).alias("avg_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("we.page")))).alias("page_values"),
    )
)
