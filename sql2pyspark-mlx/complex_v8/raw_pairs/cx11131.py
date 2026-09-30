from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("listener_id", "track_id").orderBy(F.col("played_at").desc(), F.col("id").desc())
w_listener_id_by_ms_played_buc = Window.partitionBy("listener_id").orderBy(F.col("ms_played_bucket").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_by_ms_played_buc_2 = Window.partitionBy("listener_id").orderBy(F.col("ms_played_bucket").desc(), "id")
w_listener_id = Window.partitionBy("listener_id")
w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")

prepared_plays = (
    df_plays
    .filter((F.col("device") == "car") & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "source",
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.date_trunc("day", F.col("played_at")).alias("played_at_day"),
    )
)

deduped = (
    prepared_plays
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    deduped
    .select(
        "id",
        "listener_id",
        "played_at",
        "ms_played",
        "source",
        "source_or_unknown",
        "played_at_day",
        F.last("ms_played_bucket", ignorenulls=True).over(w_listener_id_by_ms_played_buc).alias("last_known_ms_played_bucket"),
        F.row_number().over(w_listener_id_by_ms_played_buc_2).alias("ms_played_bucket_seq"),
        F.round(F.avg("ms_played_bucket").over(w_listener_id), 2).alias("listener_id_avg_ms_played_bucket"),
        F.round((F.unix_timestamp("played_at_day") - F.unix_timestamp(F.lag("played_at_day").over(w_all_by_played_at))) / 60, 1).alias("minutes_since_prev_played_at_day"),
    )
)

result = (
    ranked
    .groupingSets(
        [["listener_id", "source_or_unknown", "source"], ["listener_id"], []],
        "listener_id", "source_or_unknown", "source",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.round(F.sum("minutes_since_prev_played_at_day") / F.nullif(F.sum("ms_played"), F.lit(0)), 3).alias("minutes_since_prev_played_at_day_to_ms_played_ra"),
        F.count("*").alias("cnt"),
    )
)
