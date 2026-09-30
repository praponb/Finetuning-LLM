from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_source_len_cnt = Window.partitionBy("source_len", "cnt")
w_all_by_n_device_car_full = Window.orderBy("n_device_car", "source_len").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_n_device_car_running = Window.orderBy("n_device_car", "source_len").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_n_device_car = Window.orderBy("n_device_car", "source_len")
w_all_by_n_device_car_moving_3 = Window.orderBy("n_device_car", "source_len").rowsBetween(-3, Window.currentRow)

filtered_plays = (
    df_plays
    .filter(F.col("played_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.abs(F.col("ms_played") - 130888).alias("ms_played_distance"),
        F.length(F.trim(F.col("source"))).alias("source_len"),
    )
)

grouped = (
    filtered_plays
    .filter(F.col("track_id").isNotNull())
    .groupBy("source_len")
    .agg(
        F.sum(F.when(F.col("device") == "car", 1).otherwise(0)).alias("n_device_car"),
        F.count("*").alias("cnt"),
    )
)

scored = (
    grouped
    .withColumn("source_len_per_cnt", F.round(F.col("source_len") / F.nullif(F.col("cnt"), F.lit(0)), 3))
)

filtered = (
    scored
    .crossJoin(scored.agg(F.min("source_len_per_cnt").alias("overall_min_source_len_per_cnt")))
    .filter(F.col("source_len_per_cnt") > F.col("overall_min_source_len_per_cnt"))
    .drop("overall_min_source_len_per_cnt")
)

with_trends = (
    filtered
    .select(
        "*",
        F.round(F.avg("cnt").over(w_source_len_cnt), 2).alias("source_len_cnt_avg_cnt"),
        F.nth_value("n_device_car", 2).over(w_all_by_n_device_car_full).alias("second_n_device_car"),
        F.last("cnt", ignorenulls=True).over(w_all_by_n_device_car_running).alias("last_known_cnt"),
        F.round(F.percent_rank().over(w_all_by_n_device_car), 3).alias("n_device_car_percent_rank"),
        F.max("source_len").over(w_all_by_n_device_car_moving_3).alias("source_len_max_4"),
        F.round(F.col("source_len_per_cnt") - F.avg("source_len_per_cnt").over(w_source_len_cnt), 2).alias("source_len_per_cnt_vs_source_len_cnt_avg"),
    )
)

screened = (
    with_trends
    .crossJoin(with_trends.agg(F.avg("source_len_per_cnt_vs_source_len_cnt_avg").alias("overall_avg_source_len_per_cnt_vs_source_len_cnt")))
    .filter(F.col("source_len_per_cnt_vs_source_len_cnt_avg") > F.col("overall_avg_source_len_per_cnt_vs_source_len_cnt"))
    .drop("overall_avg_source_len_per_cnt_vs_source_len_cnt")
)

result = (
    screened
    .select(
        "source_len",
        "cnt",
        "source_len_per_cnt",
        "source_len_cnt_avg_cnt",
        "second_n_device_car",
        "last_known_cnt",
        "n_device_car_percent_rank",
        (F.col("source_len_cnt_avg_cnt") + F.col("source_len_per_cnt")).alias("source_len_cnt_avg_cnt_plus_source_len_per_cnt"),
    )
)
