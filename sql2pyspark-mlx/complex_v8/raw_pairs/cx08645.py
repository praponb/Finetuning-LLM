from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_bpm_tier_by_release_date_ful = Window.partitionBy("bpm_tier").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_release_date_running = Window.orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_bpm_tier_by_release_date = Window.partitionBy("bpm_tier").orderBy("release_date", "id")
w_bpm_tier_by_release_date_tie = Window.partitionBy("bpm_tier").orderBy("release_date")
w_artist_id_bpm_tier_by_releas = Window.partitionBy("artist_id", "bpm_tier").orderBy(F.col("release_date").asc_nulls_last(), "id")
w_bpm_tier_by_release_date_run = Window.partitionBy("bpm_tier").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_bpm_tier = Window.partitionBy("bpm_tier")
w_bpm_tier_group_by_release_da = Window.partitionBy("bpm_tier_group").orderBy("release_date_month", "id")
w_bpm_tier_group_by_release_da_2 = Window.partitionBy("bpm_tier_group").orderBy("release_date_month", "id").rowsBetween(-1, -1)

recent_tracks = (
    df_tracks
    .filter((F.col("bpm") >= 75) & (F.col("title").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "moods",
        F.when(F.col("bpm") >= 143, "HIGH").when(F.col("bpm") >= 76, "MEDIUM").otherwise("LOW").alias("bpm_tier"),
    )
)

with_trends = (
    recent_tracks
    .select(
        "*",
        F.first("release_date").over(w_bpm_tier_by_release_date_ful).alias("first_release_date"),
        F.last("release_date", ignorenulls=True).over(w_all_by_release_date_running).alias("last_known_release_date"),
        F.lead("release_date").over(w_bpm_tier_by_release_date).alias("next_release_date"),
        F.round(F.cume_dist().over(w_bpm_tier_by_release_date_tie), 3).alias("release_date_cume_dist"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_artist_id_bpm_tier_by_releas)).alias("days_since_prev_release_date"),
        F.last("release_date", ignorenulls=True).over(w_bpm_tier_by_release_date_run).alias("last_known_release_date_2"),
        F.row_number().over(w_bpm_tier_by_release_date).alias("seq_no"),
        F.dense_rank().over(w_artist_id_bpm_tier_by_releas).alias("release_date_dense_rank"),
        F.round(F.cume_dist().over(w_bpm_tier_by_release_date), 3).alias("release_date_cume_dist_2"),
        F.round(F.percent_rank().over(w_artist_id_bpm_tier_by_releas), 3).alias("release_date_percent_rank"),
    )
)

enriched = (
    with_trends
    .select(
        "*",
        F.when(F.col("bpm_tier").isin("HIGH", "MEDIUM"), "group_a").when(F.col("bpm_tier") == "LOW", "group_b").otherwise("other").alias("bpm_tier_group"),
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
        F.month(F.col("release_date")).alias("release_date_month"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.round(F.col("release_date_month") / F.nullif(F.sum("release_date_month").over(w_bpm_tier), F.lit(0)), 4).alias("release_date_month_share_of_bpm_tier"),
        F.round(F.avg("days_since_prev_release_date").over(w_bpm_tier), 2).alias("bpm_tier_avg_days_since_prev_release_date"),
        F.row_number().over(w_bpm_tier_group_by_release_da).alias("seq_no_2"),
        F.max("release_date_percent_rank").over(w_bpm_tier_group_by_release_da_2).alias("release_date_percent_rank_max_1_prior"),
    )
)

result = (
    ranked
    .filter(F.col("release_date_cume_dist") <= 0.8)
    .select(
        "id",
        "artist_id",
        "title",
        "moods",
        "bpm_tier",
        "first_release_date",
        "last_known_release_date",
        "next_release_date",
        "days_since_prev_release_date",
        "last_known_release_date_2",
        "release_date_cume_dist_2",
        "bpm_tier_group",
        "release_date_label",
        "release_date_month",
        "release_date_month_share_of_bpm_tier",
        "bpm_tier_avg_days_since_prev_release_date",
        "seq_no_2",
    )
    .orderBy(F.col("title").desc(), "last_known_release_date_2")
)
