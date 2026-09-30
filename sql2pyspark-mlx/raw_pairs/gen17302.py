from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level", "key").orderBy(F.col("new_group_flag").desc_nulls_last())
w4 = Window.partitionBy("level", "key").orderBy("new_group_flag", "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        F.explode("context").alias("key", "value"),
    )
)

cleaned = (
    exploded
    .filter((F.col("log_ts") >= F.lit("2025-06-01").cast("date")) & F.col("key").isNotNull())
    .withColumn("clean_level", F.trim("level"))
)

with_gaps = (
    cleaned
    .withColumn("new_group_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 15 MINUTES"), 1).otherwise(0))
)

sessions_app_logs = with_gaps.withColumn("session_id", F.sum("new_group_flag").over(w2))

latest = (
    sessions_app_logs
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .withColumn("prev_latency_ms", F.lag("latency_ms").over(w4))
    .withColumn("next_latency_ms", F.lead("latency_ms").over(w4))
    .withColumn("next_session_id", F.lead("session_id").over(w4))
    .withColumn("event_seq", F.row_number().over(w4))
)

result = (
    enriched
    .groupBy("next_session_id")
    .agg(
        F.min("log_ts").alias("first_seen"),
        F.bit_and("latency_ms").alias("latency_ms_bit_and"),
    )
    .orderBy(F.col("latency_ms_bit_and").desc_nulls_last())
)
