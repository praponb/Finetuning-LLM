from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_segment_by_joined_on = Window.partitionBy("segment").orderBy(F.col("joined_on").desc(), "id")
w_segment_joined_on_month_by_r = Window.partitionBy("segment", F.month("joined_on")).orderBy("risk_score", "id")
w_segment_joined_on_month_by_r_2 = Window.partitionBy("segment", F.month("joined_on")).orderBy("risk_score", "id").rowsBetween(-1, Window.currentRow)
w_segment_joined_on_month = Window.partitionBy("segment", F.date_trunc("month", "joined_on"))
w_joined_on_year_segment_by_ri = Window.partitionBy(F.year("joined_on"), "segment").orderBy("risk_score_adj", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_segment_by_joined_on_full = Window.partitionBy("segment").orderBy("joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_segment = Window.partitionBy("segment")
w_all_by_risk_score_running = Window.orderBy("risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_joined_on_year_segment_by_ri_2 = Window.partitionBy(F.year("joined_on"), "segment").orderBy("risk_score_adj", "id")

scoped_holders = (
    df_policyholders
    .filter(F.col("joined_on") < F.lit("2025-02-22").cast("date"))
    .select(
        "id",
        "holder_name",
        "risk_score",
        "joined_on",
        "segment",
        F.round(F.col("risk_score") * 1.1, 2).alias("risk_score_adj"),
        F.concat(F.col("region"), F.lit("#"), F.col("id").cast("string")).alias("region_ref"),
    )
)

with_trends = (
    scoped_holders
    .select(
        "id",
        "holder_name",
        "risk_score",
        "segment",
        "risk_score_adj",
        (F.col("risk_score_adj") - F.lag("risk_score_adj").over(w_segment_by_joined_on)).alias("risk_score_adj_change"),
        F.lag("joined_on").over(w_segment_joined_on_month_by_r).alias("prev_joined_on"),
        F.round(F.avg("risk_score").over(w_segment_joined_on_month_by_r_2), 2).alias("risk_score_avg_2"),
        F.last("risk_score_adj", ignorenulls=True).over(w_joined_on_year_segment_by_ri).alias("last_known_risk_score_adj"),
        F.first("joined_on").over(w_segment_by_joined_on_full).alias("first_joined_on"),
        F.round(F.col("risk_score") / F.nullif(F.sum("risk_score").over(w_segment), F.lit(0)), 4).alias("risk_score_share_of_segment"),
        F.round(F.col("risk_score") - F.avg("risk_score").over(w_segment_joined_on_month), 2).alias("risk_score_vs_segment_joined_on_month_avg"),
        F.sum("risk_score_adj").over(w_all_by_risk_score_running).alias("running_risk_score_adj"),
        F.lead("joined_on", 2).over(w_joined_on_year_segment_by_ri_2).alias("next2_joined_on"),
    )
)

enriched = (
    with_trends
    .select(
        "id",
        "holder_name",
        "segment",
        "risk_score_adj",
        "prev_joined_on",
        "last_known_risk_score_adj",
        "first_joined_on",
        "risk_score_share_of_segment",
        "risk_score_vs_segment_joined_on_month_avg",
        "next2_joined_on",
        F.coalesce(F.col("segment"), F.lit("UNKNOWN")).alias("segment_or_unknown"),
    )
)

grouped = (
    enriched
    .groupBy(
        F.year("prev_joined_on").alias("prev_joined_on_year"),
        "segment_or_unknown",
    )
    .agg(
        F.sum(F.when(F.col("segment").isin("SME", "RETAIL"), F.col("risk_score_adj")).otherwise(0)).alias("risk_score_adj_segment_in_sme_retail"),
        F.sum("risk_score_vs_segment_joined_on_month_avg").alias("total_risk_score_vs_segment_joined_on_month_avg"),
    )
)

result = (
    grouped
    .select(
        "prev_joined_on_year",
        "segment_or_unknown",
        "risk_score_adj_segment_in_sme_retail",
        "total_risk_score_vs_segment_joined_on_month_avg",
        F.abs(F.col("prev_joined_on_year") - 2025).alias("prev_joined_on_year_distance"),
    )
    .orderBy("segment_or_unknown")
)
