from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_artist_debut_year_run = Window.orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_artist_country_by_release_da = Window.partitionBy("artist_country").orderBy(F.col("release_date").desc(), "id")
w_artist_country_artist_id_by = Window.partitionBy("artist_country", "artist_id").orderBy(F.col("bpm").asc_nulls_last())
w_release_date_year_artist_id = Window.partitionBy(F.year("release_date"), "artist_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_country_by_release_da_2 = Window.partitionBy("artist_country").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_year_artist_id_2 = Window.partitionBy(F.year("release_date"), "artist_id").orderBy("duration_sec", "id")
w_release_date_quartile = Window.partitionBy("release_date_quartile")
w_release_date_quartile_by_seq = Window.partitionBy("release_date_quartile").orderBy("seq_no", "id")
w_artist_country_artist_id = Window.partitionBy("artist_country", "artist_id")
w_release_date_quartile_seq_no = Window.partitionBy("release_date_quartile", "seq_no").orderBy(F.col("duration_sec_vs_artist_id_avg").desc_nulls_first(), "id").rowsBetween(-6, -1)

cleaned_tracks = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

with_details = (
    cleaned_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.upper(F.trim(F.col("a.artist_name"))).like("K%")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.title_prefix",
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

with_windows = (
    with_details
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "title_prefix",
        "artist_country",
        "artist_debut_year",
        F.last("artist_debut_year", ignorenulls=True).over(w_all_by_artist_debut_year_run).alias("last_known_artist_debut_year"),
        F.min("duration_sec").over(w_artist_id).alias("artist_id_min_duration_sec"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id_is_explicit), F.lit(0)), 4).alias("duration_sec_share_of_artist_id_is_explicit"),
        F.row_number().over(w_artist_country_by_release_da).alias("seq_no"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.round(F.avg("bpm").over(w_artist_id), 2).alias("artist_id_avg_bpm"),
        F.ntile(4).over(w_artist_country_by_release_da).alias("release_date_quartile"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id_is_explicit), 2).alias("bpm_vs_artist_id_is_explicit_avg"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
        F.rank().over(w_artist_country_artist_id_by).alias("bpm_rank"),
        F.first("duration_sec", ignorenulls=True).over(w_all_by_artist_debut_year_run).alias("first_known_duration_sec"),
        F.max("bpm").over(w_release_date_year_artist_id).alias("max_bpm_so_far"),
        F.sum("duration_sec").over(w_artist_country_by_release_da_2).alias("running_duration_sec"),
        F.lag("bpm").over(w_release_date_year_artist_id_2).alias("prev_bpm"),
    )
)

sequenced = (
    with_windows
    .select(
        "*",
        F.round(F.col("last_known_artist_debut_year") - F.avg("last_known_artist_debut_year").over(w_release_date_quartile), 2).alias("last_known_artist_debut_year_vs_release_date_qua"),
        F.lag("seq_no", 1, 0).over(w_release_date_quartile_by_seq).alias("prev_seq_no"),
        F.round(F.col("bpm_rank") / F.nullif(F.sum("bpm_rank").over(w_artist_country_artist_id), F.lit(0)), 4).alias("bpm_rank_share_of_artist_country_artist_id"),
        F.sum("rows_in_artist_id").over(w_release_date_quartile_seq_no).alias("rows_in_artist_id_sum_6_prior"),
        F.round(F.col("artist_id_min_duration_sec") / F.nullif(F.sum("artist_id_min_duration_sec").over(w_artist_country_artist_id), F.lit(0)), 4).alias("artist_id_min_duration_sec_share_of_artist_count"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "artist_debut_year",
        "artist_id_min_duration_sec",
        "bpm_vs_artist_id_avg",
        "seq_no",
        "rows_in_artist_id",
        "artist_id_avg_bpm",
        "bpm_vs_artist_id_is_explicit_avg",
        "bpm_rank",
        "first_known_duration_sec",
        "max_bpm_so_far",
        "running_duration_sec",
        "prev_bpm",
        "last_known_artist_debut_year_vs_release_date_qua",
        "prev_seq_no",
        "bpm_rank_share_of_artist_country_artist_id",
        "rows_in_artist_id_sum_6_prior",
        "artist_id_min_duration_sec_share_of_artist_count",
        F.concat_ws("-", F.col("artist_country"), F.col("title")).alias("artist_country_title_label"),
    )
)
