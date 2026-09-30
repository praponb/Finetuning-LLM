from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_release_date_month_n_no = Window.partitionBy(F.month("last_release_date"), "n_not_is_explicit")
w_n_not_is_explicit_by_last_re = Window.partitionBy("n_not_is_explicit").orderBy("last_release_date", "release_date_month")

valid = (
    df_tracks
    .filter((F.col("title").isNotNull()) & (F.col("bpm") <= 166))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

grouped = (
    valid
    .groupBy(F.date_trunc("month", "release_date").alias("release_date_month"))
    .agg(
        F.max("release_date").alias("last_release_date"),
        F.max("bpm_filled").alias("max_bpm_filled"),
        F.count(F.when(~F.col("is_explicit"), 1)).alias("n_not_is_explicit"),
        F.sum("bpm").alias("sum_bpm_check"),
    )
    .filter(F.col("sum_bpm_check") > 82)
    .drop("sum_bpm_check")
)

scored = (
    grouped
    .filter(F.col("n_not_is_explicit") < 6)
    .select(
        "release_date_month",
        "last_release_date",
        "max_bpm_filled",
        "n_not_is_explicit",
        (F.col("max_bpm_filled") + F.col("n_not_is_explicit")).alias("max_bpm_filled_plus_n_not_is_explicit"),
    )
)

sequenced = (
    scored
    .select(
        "release_date_month",
        "last_release_date",
        "max_bpm_filled",
        "max_bpm_filled_plus_n_not_is_explicit",
        F.count(F.lit(1)).over(w_last_release_date_month_n_no).alias("rows_in_last_release_date_month_n_not_is_explici"),
        F.lead("max_bpm_filled", 1, 0).over(w_n_not_is_explicit_by_last_re).alias("next_max_bpm_filled"),
        F.ntile(3).over(w_n_not_is_explicit_by_last_re).alias("last_release_date_tercile"),
        (F.col("max_bpm_filled") - F.lag("max_bpm_filled").over(w_n_not_is_explicit_by_last_re)).alias("max_bpm_filled_change"),
        F.lag("n_not_is_explicit", 2).over(w_n_not_is_explicit_by_last_re).alias("prev2_n_not_is_explicit"),
    )
)

result = (
    sequenced
    .select(
        "release_date_month",
        "last_release_date",
        "max_bpm_filled",
        "max_bpm_filled_plus_n_not_is_explicit",
        "next_max_bpm_filled",
        "max_bpm_filled_change",
        "prev2_n_not_is_explicit",
    )
    .orderBy("max_bpm_filled_plus_n_not_is_explicit", "prev2_n_not_is_explicit")
)
