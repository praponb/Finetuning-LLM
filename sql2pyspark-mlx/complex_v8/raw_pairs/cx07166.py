from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_prior_ms_played_7d_mo = Window.orderBy(F.col("prior_ms_played_7d").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_all_by_prior_ms_played_7d = Window.orderBy(F.col("prior_ms_played_7d").asc_nulls_last(), "id")
w_listener_id_prior_rows_7d = Window.partitionBy("listener_id", "prior_rows_7d")
w_all_by_prior_ms_played_7d_ru = Window.orderBy(F.col("prior_ms_played_7d").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined_ranges = (
    df_plays.alias("a")
    .join(
        df_plays.alias("b"),
        (F.col("b.listener_id") == F.col("a.listener_id"))
        & (F.col("b.played_at") < F.col("a.played_at"))
        & (F.col("b.played_at") >= F.col("a.played_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.listener_id", "a.played_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.ms_played").alias("prior_ms_played_7d"),
    )
)

with_windows = (
    joined_ranges
    .select(
        "id",
        "played_at",
        "prior_rows_7d",
        F.max("prior_rows_7d").over(w_all_by_prior_ms_played_7d_mo).alias("prior_rows_7d_max_2"),
        F.row_number().over(w_all_by_prior_ms_played_7d).alias("seq_no"),
        F.first("played_at", ignorenulls=True).over(w_all_by_prior_ms_played_7d_ru).alias("first_known_played_at"),
        F.sum("prior_ms_played_7d").over(w_listener_id_prior_rows_7d).alias("listener_id_prior_rows_7d_sum_prior_ms_played_7d"),
        F.ntile(10).over(w_all_by_prior_ms_played_7d).alias("prior_ms_played_7d_decile"),
    )
)

per_group = (
    with_windows
    .groupBy("seq_no", "prior_ms_played_7d_decile")
    .agg(
        F.max("prior_ms_played_7d_decile").alias("max_prior_ms_played_7d_decile"),
        F.round(F.sum("prior_rows_7d_max_2") / F.nullif(F.sum("listener_id_prior_rows_7d_sum_prior_ms_played_7d"), F.lit(0)), 3).alias("prior_rows_7d_max_2_to_listener_id_prior_rows_7d"),
        F.min("played_at").alias("first_played_at"),
        F.round(F.stddev("prior_rows_7d"), 2).alias("stddev_prior_rows_7d"),
        F.round(F.avg("prior_rows_7d"), 2).alias("avg_prior_rows_7d"),
        F.sum("prior_ms_played_7d_decile").alias("total_prior_ms_played_7d_decile"),
        F.max("prior_rows_7d_max_2").alias("max_prior_rows_7d_max_2_check"),
    )
    .filter(F.col("max_prior_rows_7d_max_2_check") > 4)
    .drop("max_prior_rows_7d_max_2_check")
)

result = (
    per_group
    .select(
        "seq_no",
        "prior_ms_played_7d_decile",
        "avg_prior_rows_7d",
        "total_prior_ms_played_7d_decile",
        F.dayofweek(F.col("first_played_at")).alias("first_played_at_dayofweek"),
    )
    .orderBy(F.col("total_prior_ms_played_7d_decile").desc())
)
