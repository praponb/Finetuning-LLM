from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_date_year = Window.partitionBy(F.year("played_at_date"))
w_played_at_to_date_is_skipped = Window.partitionBy(F.to_date("played_at"), "is_skipped").orderBy(F.col("played_at").desc(), "id")
w_played_at_to_date_is_skipped_2 = Window.partitionBy(F.to_date("played_at"), "is_skipped").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_played_at_to_date_is_skipped_3 = Window.partitionBy(F.to_date("played_at"), "is_skipped").orderBy(F.col("played_at").desc(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_played_at = Window.orderBy(F.col("played_at").asc_nulls_last(), "id")
w_all_by_played_at_full = Window.orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_plays
    .filter((F.col("is_skipped")) & (F.col("played_at") >= F.lit("2025-01-31 10:00:00").cast("timestamp")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.to_date(F.col("played_at")).alias("played_at_date"),
    )
)

scored = (
    scoped
    .filter(F.col("played_at") >= F.lit("2025-01-19 11:00:00").cast("timestamp"))
    .select(
        "id",
        "played_at",
        "ms_played",
        "is_skipped",
        "played_at_date",
        F.date_format(F.col("played_at_date"), "yyyy-MM-dd").alias("played_at_date_label"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_played_at_date_year), F.lit(0)), 4).alias("ms_played_share_of_played_at_date_year"),
        (F.col("ms_played") - F.lag("ms_played").over(w_played_at_to_date_is_skipped)).alias("ms_played_change"),
        F.row_number().over(w_played_at_to_date_is_skipped).alias("rn"),
        F.nth_value("played_at", 2).over(w_played_at_to_date_is_skipped_2).alias("second_played_at"),
        F.sum("ms_played").over(w_played_at_to_date_is_skipped_3).alias("ms_played_sum_2"),
        F.count(F.lit(1)).over(w_played_at_date_year).alias("rows_in_played_at_date_year"),
    )
)

filtered = (
    metrics
    .crossJoin(metrics.agg(F.min("ms_played_share_of_played_at_date_year").alias("overall_min_ms_played_share_of_played_at_date_ye")))
    .filter(F.col("ms_played_share_of_played_at_date_year") > F.col("overall_min_ms_played_share_of_played_at_date_ye"))
    .drop("overall_min_ms_played_share_of_played_at_date_ye")
)

with_windows = (
    filtered
    .select(
        "*",
        F.lag("ms_played_change", 3).over(w_all_by_played_at).alias("prev3_ms_played_change"),
        F.nth_value("played_at", 2).over(w_all_by_played_at_full).alias("second_played_at_2"),
    )
)

result = (
    with_windows
    .filter(F.col("ms_played") <= 257789)
    .select(
        "id",
        "played_at",
        "ms_played",
        "played_at_date",
        "played_at_date_label",
        "ms_played_share_of_played_at_date_year",
        "ms_played_change",
        "rn",
        "second_played_at",
        "ms_played_sum_2",
        "rows_in_played_at_date_year",
        "prev3_ms_played_change",
        "second_played_at_2",
    )
    .orderBy(
        F.col("played_at_date_label").desc(),
        F.col("ms_played_share_of_played_at_date_year").asc_nulls_last(),
        "id",
    )
    .limit(20)
)
