from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_risk_score_tier_segment_by_j = Window.partitionBy("risk_score_tier", "segment").orderBy(F.col("joined_on").desc(), "id")
w_risk_score_tier_by_joined_on = Window.partitionBy("risk_score_tier").orderBy(F.col("joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_risk_score_tier_by_joined_on_2 = Window.partitionBy("risk_score_tier").orderBy(F.col("joined_on").desc())
w_risk_score_tier_segment_by_j_2 = Window.partitionBy("risk_score_tier", "segment").orderBy(F.col("joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_risk_score_tier_by_joined_on_3 = Window.partitionBy("risk_score_tier").orderBy("joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_risk_score_tier_by_joined_on_4 = Window.partitionBy("risk_score_tier").orderBy(F.col("joined_on").desc(), "id")
w_risk_score_tier_by_joined_on_5 = Window.partitionBy("risk_score_tier").orderBy("joined_on")
w_risk_score_tier_segment_by_j_3 = Window.partitionBy("risk_score_tier", "segment").orderBy(F.col("joined_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_risk_score_tier_by_joined_on_6 = Window.partitionBy("risk_score_tier").orderBy("joined_on", "id")
w_joined_on_quartile = Window.partitionBy("joined_on_quartile")
w_all_by_joined_on_dense_rank = Window.orderBy("joined_on_dense_rank", "id").rowsBetween(-6, Window.currentRow)

cleaned_holders = (
    df_policyholders
    .filter((F.col("segment") != "SME") | (F.col("region").isin("Central", "East", "West")))
    .select(
        "id",
        "holder_name",
        "joined_on",
        "segment",
        F.when(F.col("risk_score") >= 0.6, "A").when(F.col("risk_score") >= 0.3, "B").otherwise("C").alias("risk_score_tier"),
    )
)

with_windows = (
    cleaned_holders
    .select(
        "*",
        F.dense_rank().over(w_risk_score_tier_segment_by_j).alias("joined_on_dense_rank"),
        F.last("joined_on").over(w_risk_score_tier_by_joined_on).alias("last_joined_on"),
        F.round(F.percent_rank().over(w_risk_score_tier_by_joined_on_2), 3).alias("joined_on_percent_rank"),
        F.last("joined_on").over(w_risk_score_tier_segment_by_j_2).alias("last_joined_on_2"),
        F.round(F.percent_rank().over(w_risk_score_tier_segment_by_j), 3).alias("joined_on_percent_rank_2"),
        F.first("joined_on").over(w_risk_score_tier_by_joined_on_3).alias("first_joined_on"),
        F.row_number().over(w_risk_score_tier_by_joined_on_4).alias("rn"),
        F.round(F.percent_rank().over(w_risk_score_tier_by_joined_on_5), 3).alias("joined_on_percent_rank_3"),
        F.lag("joined_on").over(w_risk_score_tier_segment_by_j).alias("prev_joined_on"),
        F.first("joined_on", ignorenulls=True).over(w_risk_score_tier_segment_by_j_3).alias("first_known_joined_on"),
        F.round(F.cume_dist().over(w_risk_score_tier_by_joined_on_5), 3).alias("joined_on_cume_dist"),
        F.ntile(4).over(w_risk_score_tier_by_joined_on_6).alias("joined_on_quartile"),
    )
)

with_trends = (
    with_windows
    .select(
        "*",
        F.size(F.collect_set("segment").over(w_joined_on_quartile)).alias("distinct_segment_in_joined_on_quartile"),
        F.count(F.lit(1)).over(w_joined_on_quartile).alias("rows_in_joined_on_quartile"),
        F.round(F.col("joined_on_cume_dist") - F.avg("joined_on_cume_dist").over(w_joined_on_quartile), 2).alias("joined_on_cume_dist_vs_joined_on_quartile_avg"),
        F.round(F.col("joined_on_quartile") / F.nullif(F.sum("joined_on_quartile").over(w_joined_on_quartile), F.lit(0)), 4).alias("joined_on_quartile_share_of_joined_on_quartile"),
        F.round(F.col("joined_on_percent_rank_2") / F.nullif(F.sum("joined_on_percent_rank_2").over(w_joined_on_quartile), F.lit(0)), 4).alias("joined_on_percent_rank_2_share_of_joined_on_quar"),
        F.round(F.avg("joined_on_percent_rank").over(w_all_by_joined_on_dense_rank), 2).alias("joined_on_percent_rank_avg_7"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "holder_name",
        "joined_on",
        "segment",
        "risk_score_tier",
        "joined_on_dense_rank",
        "last_joined_on",
        "last_joined_on_2",
        "first_joined_on",
        "rn",
        "prev_joined_on",
        "first_known_joined_on",
        "joined_on_cume_dist",
        "distinct_segment_in_joined_on_quartile",
        "rows_in_joined_on_quartile",
        "joined_on_cume_dist_vs_joined_on_quartile_avg",
        "joined_on_quartile_share_of_joined_on_quartile",
        "joined_on_percent_rank_avg_7",
        F.date_format(F.col("first_known_joined_on"), "yyyy-MM-dd").alias("first_known_joined_on_label"),
    )
    .orderBy(F.col("risk_score_tier").desc())
)
