from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit_by_dur = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy("release_date", "id")
w_is_explicit_artist_id_by_rel = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").desc(), "id")
w_is_explicit = Window.partitionBy("is_explicit")
w_release_date_month_is_explic = Window.partitionBy(F.date_trunc("month", "release_date"), "is_explicit")
w_is_explicit_by_release_date = Window.partitionBy("is_explicit").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_ids_by_distinct_ids = Window.partitionBy("distinct_ids").orderBy("distinct_ids", "rn", "release_date_month").rowsBetween(-3, Window.currentRow)
w_rn_distinct_ids = Window.partitionBy("rn", "distinct_ids")

valid_tracks = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("C%"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
    )
)

metrics = (
    valid_tracks
    .select(
        "id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        "title_ref",
        F.rank().over(w_artist_id_is_explicit_by_dur).alias("duration_sec_rank"),
        F.round(F.cume_dist().over(w_artist_id_by_release_date), 3).alias("release_date_cume_dist"),
        F.lag("release_date", 2).over(w_is_explicit_artist_id_by_rel).alias("prev2_release_date"),
        F.sum("duration_sec").over(w_is_explicit).alias("is_explicit_sum_duration_sec"),
        F.lead("release_date").over(w_is_explicit_artist_id_by_rel).alias("next_release_date"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_release_date_month_is_explic), F.lit(0)), 4).alias("duration_sec_share_of_release_date_month_is_expl"),
        F.row_number().over(w_artist_id_by_release_date).alias("rn"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_artist_id_is_explicit_by_dur)).alias("duration_sec_change"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
        F.max("duration_sec").over(w_release_date_month_is_explic).alias("release_date_month_is_explicit_max_duration_sec"),
        F.last("duration_sec", ignorenulls=True).over(w_is_explicit_by_release_date).alias("last_known_duration_sec"),
        F.sum("duration_sec").over(w_release_date_month_is_explic).alias("release_date_month_is_explicit_sum_duration_sec"),
    )
)

top_per_group = metrics.filter(F.col("rn") == 1)

grouped = (
    top_per_group
    .groupBy("rn", F.date_trunc("month", "release_date").alias("release_date_month"))
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("prev2_release_date").alias("first_prev2_release_date"),
        F.round(F.avg("last_known_duration_sec"), 2).alias("avg_last_known_duration_sec"),
        F.round(F.sum("rows_in_is_explicit") / F.nullif(F.sum("duration_sec_share_of_release_date_month_is_expl"), F.lit(0)), 3).alias("rows_in_is_explicit_to_duration_sec_share_of_rel"),
    )
)

sequenced = (
    grouped
    .select(
        "*",
        F.round(F.avg("rn").over(w_distinct_ids_by_distinct_ids), 2).alias("rn_avg_4"),
        F.count(F.lit(1)).over(w_rn_distinct_ids).alias("rows_in_rn_distinct_ids"),
        F.round(F.col("rn") - F.avg("rn").over(w_rn_distinct_ids), 2).alias("rn_vs_rn_distinct_ids_avg"),
    )
)

result = (
    sequenced
    .select(
        "rn",
        "distinct_ids",
        "first_prev2_release_date",
        "avg_last_known_duration_sec",
        "rows_in_is_explicit_to_duration_sec_share_of_rel",
        "rn_avg_4",
        "rn_vs_rn_distinct_ids_avg",
        F.date_format(F.col("first_prev2_release_date"), "yyyy-MM").alias("first_prev2_release_date_label"),
    )
    .orderBy(F.col("rn_vs_rn_distinct_ids_avg").desc())
)
