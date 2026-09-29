from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_segment = Window.partitionBy("segment")
w_all_by_first_policy_start_da = Window.orderBy(F.col("first_policy_start_date").asc_nulls_last(), "id")
w_all_by_risk_score_prior_6 = Window.orderBy("risk_score", "id").rowsBetween(-6, -1)

cleaned_holders = (
    df_policyholders
    .filter((F.length(F.trim(F.col("holder_name"))) >= 7) & (F.col("region") != "West"))
    .select(
        "id",
        "region",
        "risk_score",
        "segment",
        F.date_format(F.col("joined_on"), "MMM yyyy").alias("joined_on_label"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.max("premium").alias("max_policy_premium"),
        F.min("start_date").alias("first_policy_start_date"),
    )
)

summarized_rows = (
    cleaned_holders.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select("s.*", "p.max_policy_premium", "p.first_policy_start_date")
)

windowed = (
    summarized_rows
    .select(
        "id",
        "segment",
        "first_policy_start_date",
        F.min("max_policy_premium").over(w_segment).alias("segment_min_max_policy_premium"),
        F.lag("max_policy_premium").over(w_all_by_first_policy_start_da).alias("prev_max_policy_premium"),
        F.count(F.lit(1)).over(w_segment).alias("rows_in_segment"),
        F.round(F.col("risk_score") - F.avg("risk_score").over(w_segment), 2).alias("risk_score_vs_segment_avg"),
        F.round(F.avg("max_policy_premium").over(w_all_by_risk_score_prior_6), 2).alias("max_policy_premium_avg_6_prior"),
    )
)

labeled = (
    windowed
    .filter(F.col("first_policy_start_date") < F.lit("2025-07-01").cast("date"))
    .select(
        "*",
        F.date_trunc("quarter", F.col("first_policy_start_date")).alias("first_policy_start_date_quarter"),
    )
)

result = (
    labeled
    .groupBy("first_policy_start_date_quarter", "first_policy_start_date")
    .pivot("segment", ["RETAIL", "SME", "VIP"])
    .agg(F.avg("segment_min_max_policy_premium"))
    .orderBy("first_policy_start_date_quarter", "first_policy_start_date")
)
