from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_device_by_played = Window.partitionBy("listener_id", "device").orderBy(F.col("played_at_day").desc(), "id")
w_listener_id_device_by_played_2 = Window.partitionBy("listener_id", "device").orderBy(F.col("played_at_day").desc())

base = (
    df_plays
    .filter(F.col("ms_played") > 44439)
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        F.when(F.col("device").isin("car", "desktop"), "group_a").when(F.col("device") == "phone", "group_b").otherwise("other").alias("device_group"),
        F.date_trunc("day", F.col("played_at")).alias("played_at_day"),
        F.lower(F.regexp_replace(F.trim(F.col("device")), " ", "_")).alias("device_slug"),
    )
)

ranked = (
    base
    .select(
        "*",
        F.lag("played_at_day").over(w_listener_id_device_by_played).alias("prev_played_at_day"),
        F.round((F.unix_timestamp("played_at") - F.unix_timestamp(F.lag("played_at").over(w_listener_id_device_by_played))) / 60, 1).alias("minutes_since_prev_played_at"),
        F.rank().over(w_listener_id_device_by_played_2).alias("played_at_day_rank"),
        F.dense_rank().over(w_listener_id_device_by_played).alias("played_at_day_dense_rank"),
        F.round(F.percent_rank().over(w_listener_id_device_by_played), 3).alias("played_at_day_percent_rank"),
    )
)

grouped = (
    ranked
    .filter(F.col("minutes_since_prev_played_at") > 2000.0)
    .groupBy(F.year("played_at_day").alias("played_at_day_year"))
    .agg(
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.percentile_approx("played_at_day_percent_rank", 0.5).alias("median_played_at_day_percent_rank"),
        F.count("*").alias("n_rows"),
        F.max("played_at").alias("last_played_at"),
    )
    .filter(F.col("n_rows") >= 3)
)

tagged = (
    grouped
    .withColumn("last_played_at_quarter", F.date_trunc("quarter", F.col("last_played_at")))
)

result = (
    tagged
    .filter(F.col("n_rows") <= 10)
    .select(
        "played_at_day_year",
        "source_list",
        "median_played_at_day_percent_rank",
        "n_rows",
        "last_played_at_quarter",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("last_played_at")).alias("days_since_last_played_at"),
        F.when(F.col("n_rows") >= 7, "large").when(F.col("n_rows") >= 5, "medium").otherwise("small").alias("n_rows_tier"),
    )
    .orderBy(F.col("n_rows").desc(), F.col("last_played_at_quarter").desc())
)
