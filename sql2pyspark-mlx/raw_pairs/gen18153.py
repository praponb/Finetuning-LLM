from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host", "attr_key").orderBy("log_ts", "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key")
w3 = Window.partitionBy("attr_key").orderBy("log_ts", "id", "attr_key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("attr_key").orderBy(F.col("rank_in_group").desc(), "id", "attr_key")

flattened = (
    df_app_logs
    .select(
        "id",
        "host",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("attr_key", "value"),
    )
)

ranked = (
    flattened
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

flagged = (
    ranked
    .withColumn("new_group_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w2) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "log_ts",
        "latency_ms",
        "attr_key",
        "value",
        "rank_in_group",
        F.sum("new_group_flag").over(w3).alias("session_no"),
    )
)

latest = (
    sessionized
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select("id", "attr_key", "value", "rank_in_group", "session_no")
)

result = (
    latest
    .groupBy(
        F.when(F.col("session_no") >= 6, "PREMIUM").otherwise("STANDARD").alias("session_no_tier"),
    )
    .agg(
        F.round(F.avg("session_no"), 2).alias("avg_session_no"),
        F.round(F.avg("session_no"), 2).alias("avg_session_no_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("attr_key") == "version").alias("version_events"),
    )
)
