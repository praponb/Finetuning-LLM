from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played = Window.orderBy(F.col("ms_played").asc_nulls_last(), "id")
w_is_skipped = Window.partitionBy("is_skipped")
w_all_by_played_at_ties = Window.orderBy("played_at")
w_all_by_played_at = Window.orderBy("played_at", "id")
w_is_skipped_by_played_at_runn = Window.partitionBy("is_skipped").orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_played_at_prior_1 = Window.orderBy("played_at", "id").rowsBetween(-1, -1)

prepared_plays = (
    df_plays
    .filter(~F.col("is_skipped"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
    )
)

windowed = (
    prepared_plays
    .select(
        "id",
        "played_at",
        "is_skipped",
        "source",
        F.row_number().over(w_all_by_ms_played).alias("seq_no"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_is_skipped), 2).alias("ms_played_vs_is_skipped_avg"),
        F.round(F.percent_rank().over(w_all_by_ms_played), 3).alias("ms_played_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_played_at_ties), 3).alias("played_at_percent_rank"),
        F.row_number().over(w_all_by_played_at).alias("rn"),
        F.rank().over(w_all_by_played_at).alias("played_at_rank"),
        F.first("ms_played", ignorenulls=True).over(w_is_skipped_by_played_at_runn).alias("first_known_ms_played"),
        F.round(F.avg("ms_played").over(w_all_by_played_at_prior_1), 2).alias("ms_played_avg_1_prior"),
        F.round(F.cume_dist().over(w_all_by_ms_played), 3).alias("ms_played_cume_dist"),
    )
)

annotated = (
    windowed
    .select(
        "*",
        F.when(F.col("source").isin("playlist", "search"), "group_a").when(F.col("source") == "radio", "group_b").otherwise("other").alias("source_group"),
        F.when(F.col("source").isin("search", "album"), "group_a").when(F.col("source") == "playlist", "group_b").otherwise("other").alias("source_group_2"),
        (F.col("ms_played_cume_dist") + F.col("ms_played_avg_1_prior")).alias("ms_played_cume_dist_plus_ms_played_avg_1_prior"),
    )
)

grouped = (
    annotated
    .groupBy("is_skipped")
    .agg(
        F.min("seq_no").alias("min_seq_no"),
        F.countDistinct("source_group_2").alias("distinct_source_group_2s"),
        F.avg("played_at_rank").alias("avg_played_at_rank_check"),
    )
    .filter(F.col("avg_played_at_rank_check") > 4)
    .drop("avg_played_at_rank_check")
)

result = (
    grouped
    .select("is_skipped", "min_seq_no", "distinct_source_group_2s")
    .orderBy(
        F.col("distinct_source_group_2s").desc(),
        F.col("min_seq_no").asc_nulls_last(),
    )
)
