from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.device"))
    .agg(
        F.sum(F.when(F.col("we.page") == "/checkout", 1).otherwise(0)).alias("checkout_count"),
    )
    .filter(F.col("checkout_count") > 43)
)
