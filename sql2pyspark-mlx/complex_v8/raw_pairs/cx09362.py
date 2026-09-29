from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_segment_group_by_joined_on_p = Window.partitionBy("segment_group").orderBy(F.col("joined_on").desc(), "id").rowsBetween(-3, -1)
w_segment_group_by_risk_score = Window.partitionBy("segment_group").orderBy("risk_score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_segment_by_joined_on_full = Window.partitionBy("segment").orderBy("joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_segment = Window.partitionBy("segment")
w_segment_group = Window.partitionBy("segment_group")
w_segment_group_by_joined_on = Window.partitionBy("segment_group").orderBy("joined_on", "id")

recent_holders = (
    df_policyholders
    .select(
        "id",
        "holder_name",
        "risk_score",
        "joined_on",
        "segment",
        F.concat_ws(": ", F.col("holder_name"), F.col("region")).alias("holder_name_region_label"),
        F.when(F.col("segment").isin("SME", "RETAIL"), "group_a").when(F.col("segment") == "VIP", "group_b").otherwise("other").alias("segment_group"),
    )
)

sequenced = (
    recent_holders
    .select(
        "*",
        F.round(F.avg("risk_score").over(w_segment_group_by_joined_on_p), 2).alias("risk_score_avg_3_prior"),
        F.first("risk_score", ignorenulls=True).over(w_segment_group_by_risk_score).alias("first_known_risk_score"),
        F.first("joined_on").over(w_segment_by_joined_on_full).alias("first_joined_on"),
        F.max("risk_score").over(w_segment).alias("segment_max_risk_score"),
        F.size(F.collect_set("segment_group").over(w_segment)).alias("distinct_segment_group_in_segment"),
        F.round(F.avg("risk_score").over(w_segment), 2).alias("segment_avg_risk_score"),
        F.max("risk_score").over(w_segment_group).alias("segment_group_max_risk_score"),
        F.ntile(3).over(w_segment_group_by_joined_on).alias("joined_on_tercile"),
    )
)

per_group = (
    sequenced
    .groupBy(
        "segment_group",
        F.date_trunc("month", "joined_on").alias("joined_on_month"),
        "distinct_segment_group_in_segment",
    )
    .agg(
        F.round(F.sum("segment_group_max_risk_score") / F.nullif(F.sum("risk_score"), F.lit(0)), 3).alias("segment_group_max_risk_score_to_risk_score_ratio"),
        F.sum(F.when(F.col("segment") != "RETAIL", 1).otherwise(0)).alias("n_segment_retail"),
        F.sum("risk_score").alias("sum_risk_score_check"),
    )
    .filter(F.col("sum_risk_score_check") > 0.1)
    .drop("sum_risk_score_check")
)

result = (
    per_group
    .select(
        "segment_group",
        "joined_on_month",
        "segment_group_max_risk_score_to_risk_score_ratio",
        "n_segment_retail",
    )
    .orderBy("joined_on_month")
)
