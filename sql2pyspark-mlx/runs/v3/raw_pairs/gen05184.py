from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("s.browser").isin(["Safari", "Firefox"]), "group_a").otherwise("other").alias("browser_group"),
    )
    .agg(F.sum(F.when(F.col("we.page") == "/home", 1).otherwise(0)).alias("home_count"))
)
