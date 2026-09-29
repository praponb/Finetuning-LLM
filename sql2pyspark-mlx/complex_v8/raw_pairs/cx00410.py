from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_played_at_year = Window.partitionBy(F.year("played_at"))
w_source_device_by_played_at_f = Window.partitionBy("source", "device").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_played_at_tie = Window.partitionBy("listener_id").orderBy("played_at")
w_device_source_by_played_at = Window.partitionBy("device", "source").orderBy("played_at", "id")
w_device_source_by_played_at_f = Window.partitionBy("device", "source").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_by_played_at_run = Window.partitionBy("listener_id").orderBy("played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id_by_played_at = Window.partitionBy("listener_id").orderBy("played_at", "id")
w_seq_no_by_rows_in_played_at = Window.partitionBy("seq_no").orderBy("rows_in_played_at_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_listener_id_in_play = Window.partitionBy("distinct_listener_id_in_played_at_year").orderBy("first_played_at", "id")
w_seq_no_by_rows_in_played_at_2 = Window.partitionBy("seq_no").orderBy("rows_in_played_at_year")

scoped_plays = (
    df_plays
    .filter(F.col("device").isin("desktop", "tv", "speaker"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
    )
)

windowed = (
    scoped_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.count(F.lit(1)).over(w_played_at_year).alias("rows_in_played_at_year"),
        F.nth_value("played_at", 2).over(w_source_device_by_played_at_f).alias("second_played_at"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_played_at_year), 2).alias("ms_played_vs_played_at_year_avg"),
        F.rank().over(w_listener_id_by_played_at_tie).alias("played_at_rank"),
        F.row_number().over(w_device_source_by_played_at).alias("seq_no"),
        F.size(F.collect_set("listener_id").over(w_played_at_year)).alias("distinct_listener_id_in_played_at_year"),
        F.max("ms_played").over(w_played_at_year).alias("played_at_year_max_ms_played"),
        F.first("played_at").over(w_device_source_by_played_at_f).alias("first_played_at"),
        F.first("played_at", ignorenulls=True).over(w_listener_id_by_played_at_run).alias("first_known_played_at"),
        F.round(F.percent_rank().over(w_listener_id_by_played_at), 3).alias("played_at_percent_rank"),
        F.round(F.percent_rank().over(w_listener_id_by_played_at_tie), 3).alias("played_at_percent_rank_2"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.first("played_at", ignorenulls=True).over(w_seq_no_by_rows_in_played_at).alias("first_known_played_at_2"),
        F.lag("rows_in_played_at_year", 1, 0).over(w_distinct_listener_id_in_play).alias("prev_rows_in_played_at_year"),
        F.round(F.percent_rank().over(w_seq_no_by_rows_in_played_at_2), 3).alias("rows_in_played_at_year_percent_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "played_at",
        "ms_played",
        "source",
        "rows_in_played_at_year",
        "ms_played_vs_played_at_year_avg",
        "seq_no",
        "played_at_year_max_ms_played",
        "first_played_at",
        "first_known_played_at",
        "played_at_percent_rank",
        "first_known_played_at_2",
        "rows_in_played_at_year_percent_rank",
    )
    .orderBy(F.col("ms_played_vs_played_at_year_avg").desc())
)
