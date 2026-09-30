from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_joined_on_month_joined_on_mo = Window.partitionBy("joined_on_month", F.month("joined_on")).orderBy("joined_on")
w_all_by_joined_on_month = Window.orderBy("joined_on_month", "id")
w_joined_on_month_joined_on_mo_2 = Window.partitionBy("joined_on_month", F.month("joined_on")).orderBy("joined_on", "id")
w_all_by_risk_score = Window.orderBy("risk_score", "id")
w_segment_by_risk_score = Window.partitionBy("segment").orderBy(F.col("risk_score").asc_nulls_last(), "id")
w_segment_joined_on_month = Window.partitionBy("segment", "joined_on_month")
w_segment = Window.partitionBy("segment")
w_all_by_joined_on_month_runni = Window.orderBy("joined_on_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_joined_on_dense_rank_by_firs = Window.partitionBy("joined_on_dense_rank").orderBy(F.col("first_known_joined_on").desc_nulls_first(), "id")
w_joined_on_dense_rank_by_risk = Window.partitionBy("joined_on_dense_rank").orderBy("risk_score")
w_joined_on_month_decile = Window.partitionBy("joined_on_month_decile")

valid = (
    df_policyholders
    .filter((F.col("risk_score") <= 0.8) & (F.length(F.trim(F.col("holder_name"))) >= 7))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.month(F.col("joined_on")).alias("joined_on_month"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.round(F.percent_rank().over(w_joined_on_month_joined_on_mo), 3).alias("joined_on_percent_rank"),
        F.ntile(10).over(w_all_by_joined_on_month).alias("joined_on_month_decile"),
        F.lead("risk_score").over(w_joined_on_month_joined_on_mo_2).alias("next_risk_score"),
        F.ntile(5).over(w_all_by_risk_score).alias("risk_score_quintile"),
        F.lag("risk_score").over(w_segment_by_risk_score).alias("prev_risk_score"),
        F.max("risk_score").over(w_segment_joined_on_month).alias("segment_joined_on_month_max_risk_score"),
        F.round(F.avg("joined_on_month").over(w_segment), 2).alias("segment_avg_joined_on_month"),
        F.round(F.col("risk_score") / F.nullif(F.sum("risk_score").over(w_segment), F.lit(0)), 4).alias("risk_score_share_of_segment"),
        F.dense_rank().over(w_joined_on_month_joined_on_mo).alias("joined_on_dense_rank"),
        F.dense_rank().over(w_all_by_risk_score).alias("risk_score_dense_rank"),
        F.first("joined_on", ignorenulls=True).over(w_all_by_joined_on_month_runni).alias("first_known_joined_on"),
        F.sum("risk_score").over(w_segment).alias("segment_sum_risk_score"),
    )
)

top_per_group = windowed.filter(F.col("risk_score_dense_rank") <= 5)

sequenced = (
    top_per_group
    .select(
        "id",
        "holder_name",
        "region",
        "segment",
        "next_risk_score",
        "risk_score_quintile",
        "segment_joined_on_month_max_risk_score",
        "segment_avg_joined_on_month",
        "risk_score_share_of_segment",
        "joined_on_dense_rank",
        "risk_score_dense_rank",
        F.lead("joined_on_month", 2).over(w_joined_on_dense_rank_by_firs).alias("next2_joined_on_month"),
        (F.col("joined_on_month") - F.lag("joined_on_month").over(w_joined_on_dense_rank_by_firs)).alias("joined_on_month_change"),
        F.dense_rank().over(w_joined_on_dense_rank_by_risk).alias("risk_score_dense_rank_2"),
        F.round(F.col("risk_score_dense_rank") - F.avg("risk_score_dense_rank").over(w_joined_on_month_decile), 2).alias("risk_score_dense_rank_vs_joined_on_month_decile_"),
        F.row_number().over(w_joined_on_dense_rank_by_firs).alias("first_known_joined_on_seq"),
    )
)

result = (
    sequenced
    .filter(F.col("holder_name").isNotNull())
    .select(
        "id",
        "region",
        "segment",
        "risk_score_quintile",
        "segment_joined_on_month_max_risk_score",
        "segment_avg_joined_on_month",
        "risk_score_share_of_segment",
        "joined_on_dense_rank",
        "risk_score_dense_rank",
        "joined_on_month_change",
        "risk_score_dense_rank_2",
        "first_known_joined_on_seq",
    )
    .orderBy(F.col("risk_score_dense_rank_2").desc(), "joined_on_dense_rank")
)
