from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("host").orderBy("log_ts", "id", "key")
w2 = Window.partitionBy("host").orderBy("log_ts", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy("island_id", "id", "key").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("level", "key").orderBy(F.col("session_start_flag").desc(), "id", "key")
w5 = Window.partitionBy("service", "level").orderBy("session_start_flag", "id", "key")
w6 = Window.partitionBy("level").orderBy("island_id")
w7 = Window.partitionBy("host", "service").orderBy("session_start_flag_bucket", "id", "key")

exploded = (
    df_app_logs
    .select(
        "id",
        "service",
        "host",
        "log_ts",
        "level",
        "status_code",
        F.explode("context").alias("key", "v"),
    )
)

with_gaps = (
    exploded
    .withColumn("session_start_flag", F.when(F.col("log_ts") - F.lag("log_ts").over(w) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

with_windows = (
    sessionized
    .withColumn("first_known_key", F.first("key", ignorenulls=True).over(w3))
    .withColumn("session_start_flag_bucket", F.ntile(4).over(w4))
    .withColumn("next_status_code", F.lead("status_code", 1, 0).over(w5))
    .withColumn("island_id_percent_rank", F.percent_rank().over(w6))
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

daily = (
    ranked
    .groupBy(F.date_trunc("hour", "log_ts").alias("hour_window"))
    .agg(
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_count"),
        F.avg(F.when(F.col("host") == "h4", F.col("status_code"))).alias("h4_avg_status_code"),
        F.avg(F.when(F.col("key") == "trace", F.col("session_start_flag"))).alias("trace_avg_session_start_flag"),
        F.min("island_id_percent_rank").alias("min_island_id_percent_rank"),
        F.count(F.when(F.col("level") == "DEBUG", 1)).alias("debug_count_2"),
    )
)

result = (
    daily
    .select(
        "hour_window",
        "h4_avg_status_code",
        "trace_avg_session_start_flag",
        "min_island_id_percent_rank",
    )
)
