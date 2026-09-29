from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("log_ts", "id", "k")
w2 = Window.partitionBy("level").orderBy("log_ts", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("level").orderBy("burst_id", "id", "k")

exploded = (
    df_app_logs
    .select(
        "id",
        "log_ts",
        "level",
        "latency_ms",
        "status_code",
        F.explode("context").alias("k", "attr_value"),
    )
)

flagged = (
    exploded
    .withColumn("new_group_flag", F.when(F.lag("log_ts").over(w).isNull() | (F.col("log_ts") > F.lag("log_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

deduped = (
    islands
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

with_prev = deduped.withColumn("prev_log_ts", F.lag("log_ts").over(w))

with_gaps = (
    with_prev
    .withColumn("new_group_flag_2", F.when(F.col("prev_log_ts").isNull() | (F.col("log_ts") > F.col("prev_log_ts") + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_id", F.sum("new_group_flag_2").over(w2))

result = (
    sessionized
    .withColumn("hour_window", F.date_trunc("hour", "log_ts"))
    .withColumn("year_start", F.date_trunc("year", "prev_log_ts"))
    .withColumn("activity_day", F.date_trunc("day", "log_ts"))
    .groupingSets(
        [["hour_window", "year_start", "activity_day"], ["hour_window", "year_start"], []],
        "hour_window",
        "year_start",
        "activity_day",
    )
    .agg(
        F.sum("status_code").alias("total_status_code"),
        F.round(F.sum("new_group_flag") / F.nullif(F.count(F.when(F.col("k") == "region", 1)), F.lit(0)), 2).alias("new_group_flag_per_region"),
        F.count(F.when(F.col("k") == "version", 1)).alias("version_count"),
        F.grouping("hour_window").alias("grouping_level"),
    )
    .filter(F.col("version_count") > 1)
)
