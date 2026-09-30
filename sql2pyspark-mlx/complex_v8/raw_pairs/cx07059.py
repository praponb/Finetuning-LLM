from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_duration_sec_ti = Window.partitionBy("artist_id").orderBy("duration_sec")
w_all_by_duration_sec_prior_1 = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-1, -1)
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_duration_sec_ru = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_duration_sec_pr = Window.partitionBy("artist_id").orderBy("duration_sec", "id").rowsBetween(-6, -1)
w_all_by_duration_sec_moving_1 = Window.orderBy("duration_sec", "id").rowsBetween(-1, Window.currentRow)
w_artist_id_by_duration_sec_mo = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc(), "id")
w_artist_id_by_first_known_dur = Window.partitionBy("artist_id").orderBy("first_known_duration_sec")
w_artist_id_by_first_known_dur_2 = Window.partitionBy("artist_id").orderBy("first_known_duration_sec", "id")
w_artist_id_by_duration_sec_av = Window.partitionBy("artist_id").orderBy("duration_sec_avg_2_adj", "id")
w_all_by_artist_id_max_duratio = Window.orderBy("artist_id_max_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_tracks = (
    df_tracks
    .filter(F.col("duration_sec") < 402)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
    )
)

with_trends = (
    valid_tracks
    .select(
        "*",
        F.round(F.percent_rank().over(w_artist_id_by_duration_sec_ti), 3).alias("duration_sec_percent_rank"),
        F.max("duration_sec").over(w_all_by_duration_sec_prior_1).alias("duration_sec_max_1_prior"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
        F.first("duration_sec", ignorenulls=True).over(w_artist_id_by_duration_sec_ru).alias("first_known_duration_sec"),
        F.max("duration_sec").over(w_artist_id).alias("artist_id_max_duration_sec"),
        F.sum("duration_sec").over(w_artist_id_by_duration_sec_pr).alias("duration_sec_sum_6_prior"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_artist_id"),
        F.round(F.avg("duration_sec").over(w_all_by_duration_sec_moving_1), 2).alias("duration_sec_avg_2"),
        F.max("duration_sec").over(w_artist_id_by_duration_sec_mo).alias("duration_sec_max_7"),
        F.round(F.percent_rank().over(w_all_by_duration_sec), 3).alias("duration_sec_percent_rank_2"),
    )
)

labeled = (
    with_trends
    .select(
        "*",
        F.round(F.col("duration_sec_avg_2") * 1.07, 2).alias("duration_sec_avg_2_adj"),
        F.abs(F.col("duration_sec_max_7") - 558).alias("duration_sec_max_7_distance"),
    )
)

ranked = (
    labeled
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "duration_sec_max_1_prior",
        "duration_sec_vs_artist_id_avg",
        "artist_id_max_duration_sec",
        "duration_sec_sum_6_prior",
        "duration_sec_share_of_artist_id",
        "duration_sec_avg_2",
        "duration_sec_percent_rank_2",
        "duration_sec_avg_2_adj",
        "duration_sec_max_7_distance",
        F.round(F.cume_dist().over(w_artist_id_by_first_known_dur), 3).alias("first_known_duration_sec_cume_dist"),
        F.row_number().over(w_artist_id_by_first_known_dur_2).alias("rn"),
        F.ntile(5).over(w_artist_id_by_duration_sec_av).alias("duration_sec_avg_2_adj_quintile"),
        F.count(F.lit(1)).over(w_all_by_artist_id_max_duratio).alias("running_count"),
        F.ntile(10).over(w_artist_id_by_first_known_dur_2).alias("first_known_duration_sec_decile"),
        F.last("duration_sec", ignorenulls=True).over(w_all_by_artist_id_max_duratio).alias("last_known_duration_sec"),
        F.lead("duration_sec_avg_2_adj", 1, 0).over(w_artist_id_by_duration_sec_av).alias("next_duration_sec_avg_2_adj"),
    )
)

grouped = (
    ranked
    .groupBy("rn")
    .agg(
        F.array_sort(F.collect_set("title")).alias("title_list"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    grouped
    .filter(F.col("distinct_ids") <= 7)
    .select(
        "rn",
        "title_list",
        "distinct_artist_ids",
        "distinct_ids",
        F.when(F.col("rn") >= 7, "A").when(F.col("rn") >= 4, "B").otherwise("C").alias("rn_tier"),
    )
    .orderBy("rn_tier", F.col("distinct_ids").desc_nulls_first())
)
