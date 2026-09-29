from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_n_title_is_not_null = Window.orderBy("n_title_is_not_null", "is_explicit", "title_len")
w_all_by_n_title_is_not_null_f = Window.orderBy("n_title_is_not_null", "is_explicit", "title_len").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_n_title_is_not_null_r = Window.orderBy("n_title_is_not_null", "is_explicit", "title_len").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_n_title_is_not_null_m = Window.orderBy("n_title_is_not_null", "is_explicit", "title_len").rowsBetween(-1, Window.currentRow)
w_is_explicit_title_len = Window.partitionBy("is_explicit", "title_len")

cleaned = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 1) & (F.col("duration_sec") < 352))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.max("played_at").alias("last_played_at_2"),
    )
)

with_stats = (
    cleaned.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.last_played_at", "p.last_played_at_2")
)

summary = (
    with_stats
    .groupBy("is_explicit", "title_len")
    .agg(
        F.round(F.avg("bpm"), 2).alias("avg_bpm"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("title").isNotNull(), 1).otherwise(0)).alias("n_title_is_not_null"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .filter(F.col("cnt") >= 3)
)

ranked = (
    summary
    .select(
        "is_explicit",
        "title_len",
        "avg_bpm",
        "n_title_is_not_null",
        "min_duration_sec",
        F.ntile(3).over(w_all_by_n_title_is_not_null).alias("n_title_is_not_null_tercile"),
        F.last("avg_bpm").over(w_all_by_n_title_is_not_null_f).alias("last_avg_bpm"),
        F.first("title_len", ignorenulls=True).over(w_all_by_n_title_is_not_null_r).alias("first_known_title_len"),
        F.sum("n_title_is_not_null").over(w_all_by_n_title_is_not_null_m).alias("n_title_is_not_null_sum_2"),
        F.count(F.lit(1)).over(w_is_explicit_title_len).alias("rows_in_is_explicit_title_len"),
    )
)

result = (
    ranked
    .select(
        "is_explicit",
        "title_len",
        "avg_bpm",
        "last_avg_bpm",
    )
    .unpivot(
        ids=["is_explicit", "title_len"],
        values=["avg_bpm", "last_avg_bpm"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
