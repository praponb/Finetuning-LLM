from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_month_label_len_is = Window.partitionBy("played_at_month_label_len", "is_skipped").orderBy(F.col("track_release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_played_at_month_label_len_is_2 = Window.partitionBy("played_at_month_label_len", "is_skipped").orderBy(F.col("track_release_date").desc())
w_played_at_month_label_len_is_3 = Window.partitionBy("played_at_month_label_len", "is_skipped").orderBy(F.col("track_release_date").desc(), "id")
w_is_skipped_played_at_month_l = Window.partitionBy("is_skipped", "played_at_month_label_len")
w_played_at_month_label_len_is_4 = Window.partitionBy("played_at_month_label_len", "is_skipped").orderBy(F.col("track_release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_plays
    .filter(F.col("source") == "radio")
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "is_skipped",
        F.date_trunc("month", F.col("played_at")).alias("played_at_month"),
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

annotated = (
    base
    .select(
        "id",
        "track_id",
        "played_at",
        "is_skipped",
        "played_at_month",
        "device_clean",
        "played_at_date",
        F.date_format(F.col("played_at_month"), "yyyy-MM-dd").alias("played_at_month_label"),
    )
)

tagged = (
    annotated
    .filter(F.col("is_skipped"))
    .select(
        "id",
        "track_id",
        "is_skipped",
        "device_clean",
        "played_at_month_label",
        F.length(F.trim(F.col("played_at_month_label"))).alias("played_at_month_label_len"),
    )
)

with_parent = (
    tagged.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.length(F.trim(F.col("t.title"))) >= 5), "left")
    .select(
        "s.id",
        "s.track_id",
        "s.is_skipped",
        "s.device_clean",
        "s.played_at_month_label",
        "s.played_at_month_label_len",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.title").alias("track_title"),
    )
)

ranked = (
    with_parent
    .select(
        "id",
        "track_id",
        "is_skipped",
        "device_clean",
        "played_at_month_label",
        "played_at_month_label_len",
        "track_release_date",
        "track_moods",
        "track_title",
        F.last("track_release_date", ignorenulls=True).over(w_played_at_month_label_len_is).alias("last_known_track_release_date"),
        F.dense_rank().over(w_played_at_month_label_len_is_2).alias("track_release_date_dense_rank"),
        F.lead("played_at_month_label_len", 3).over(w_played_at_month_label_len_is_3).alias("next3_played_at_month_label_len"),
        F.row_number().over(w_played_at_month_label_len_is_3).alias("track_release_date_seq"),
        F.count(F.lit(1)).over(w_is_skipped_played_at_month_l).alias("rows_in_is_skipped_played_at_month_label_len"),
        F.nth_value("track_release_date", 2).over(w_played_at_month_label_len_is_4).alias("second_track_release_date"),
    )
)

scored = (
    ranked
    .filter(F.col("rows_in_is_skipped_played_at_month_label_len").between(7, 16))
    .select(
        "*",
        F.quarter(F.col("track_release_date")).alias("track_release_date_quarter"),
        F.add_months(F.col("track_release_date"), 3).alias("track_release_date_plus_3m"),
        F.substring(F.trim(F.col("device_clean")), 1, 3).alias("device_clean_prefix"),
    )
)

result = (
    scored
    .select(
        "id",
        "is_skipped",
        "played_at_month_label",
        "played_at_month_label_len",
        "track_moods",
        "track_title",
        "track_release_date_dense_rank",
        "next3_played_at_month_label_len",
        "track_release_date_seq",
        "second_track_release_date",
        "track_release_date_quarter",
        "track_release_date_plus_3m",
        "device_clean_prefix",
        F.concat_ws("|", F.col("played_at_month_label"), F.col("track_title")).alias("played_at_month_label_track_title_label"),
        F.date_format(F.col("track_release_date"), "yyyy-MM").alias("track_release_date_label"),
    )
    .orderBy(
        F.col("track_release_date_dense_rank").desc_nulls_first(),
        F.col("device_clean_prefix").asc(),
        "id",
    )
    .limit(10)
)
