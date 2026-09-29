from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_segment = Window.partitionBy("segment")

valid_holders = (
    df_policyholders
    .filter((F.col("holder_name").isNotNull()) & (F.col("joined_on").between(F.lit("2024-06-18").cast("date"), F.lit("2025-10-01").cast("date"))))
    .select(
        "id",
        "risk_score",
        "joined_on",
        "segment",
    )
)

with_windows = (
    valid_holders
    .select(
        "*",
        F.sum("risk_score").over(w_segment).alias("segment_sum_risk_score"),
        F.count(F.lit(1)).over(w_segment).alias("rows_in_segment"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.max("start_date").alias("last_policy_start_date"),
        F.count(F.when(F.col("status") != "EXPIRED", 1)).alias("policy_status_count"),
        F.max("end_date").alias("last_policy_end_date"),
    )
)

summarized_rows = (
    with_windows.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.last_policy_start_date",
        F.coalesce(F.col("p.policy_status_count"), F.lit(0)).alias("policy_status_count"),
        "p.last_policy_end_date",
    )
)

policy_stats_2 = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count(F.when(F.col("status") == "LAPSED", 1)).alias("policy_status_count_2"),
        F.max("premium").alias("max_policy_premium"),
        F.count(F.when(~F.col("product").isin("HEALTH", "AUTO"), 1)).alias("policy_product_count"),
    )
)

with_stats = (
    summarized_rows.alias("s")
    .join(policy_stats_2.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_status_count_2"), F.lit(0)).alias("policy_status_count_2"),
        "p.max_policy_premium",
        F.coalesce(F.col("p.policy_product_count"), F.lit(0)).alias("policy_product_count"),
    )
)

scored = (
    with_stats
    .select(
        "*",
        F.date_format(F.col("last_policy_start_date"), "yyyy-MM").alias("last_policy_start_date_label"),
        F.abs(F.col("policy_status_count_2") - 4).alias("policy_status_count_2_distance"),
    )
)

summary = (
    scored
    .filter(F.col("policy_status_count_2_distance") > 2)
    .groupBy(
        F.date_trunc("month", "last_policy_start_date").alias("last_policy_start_date_month"),
        "policy_status_count",
        "policy_status_count_2_distance",
    )
    .agg(
        F.max("max_policy_premium").alias("max_max_policy_premium"),
        F.max("policy_status_count").alias("max_policy_status_count"),
        F.round(100 * F.count(F.when(~F.col("segment").isin("RETAIL", "SME"), 1)) / F.count("*"), 1).alias("pct_segment_not_in_retail_sme"),
        F.max("segment_sum_risk_score").alias("max_segment_sum_risk_score_check"),
    )
    .filter(F.col("max_segment_sum_risk_score_check") > 1.0)
    .drop("max_segment_sum_risk_score_check")
)

tagged = (
    summary
    .select(
        "*",
        F.abs(F.col("max_policy_status_count") - 4).alias("max_policy_status_count_distance"),
        F.when(F.col("max_policy_status_count") >= 6, "HIGH").when(F.col("max_policy_status_count") >= 3, "MEDIUM").otherwise("LOW").alias("max_policy_status_count_tier"),
    )
)

result = (
    tagged
    .select(
        "last_policy_start_date_month",
        "policy_status_count",
        "policy_status_count_2_distance",
        "max_max_policy_premium",
        "pct_segment_not_in_retail_sme",
        "max_policy_status_count_tier",
    )
    .orderBy(
        F.col("policy_status_count_2_distance").desc(),
        F.col("pct_segment_not_in_retail_sme").desc(),
    )
)
