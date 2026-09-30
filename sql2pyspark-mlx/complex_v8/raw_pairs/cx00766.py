from pyspark.sql import functions as F

filtered = (
    df_policies
    .filter(F.col("end_date").between(F.lit("2024-11-08").cast("date"), F.lit("2026-09-19").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.max("start_date").alias("last_policy_start_date"),
        F.min("end_date").alias("first_policy_end_date"),
        F.min("end_date").alias("first_policy_end_date_2"),
    )
)

summarized_rows = (
    filtered.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.last_policy_start_date",
        "p.first_policy_end_date",
        "p.first_policy_end_date_2",
    )
)

with_details_holder = (
    summarized_rows.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.risk_score") < 0.6), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.status_prefix",
        "s.last_policy_start_date",
        "s.first_policy_end_date_2",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

combined_details = (
    with_details_holder.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.status_prefix",
        "s.last_policy_start_date",
        "s.holder_segment",
        "s.holder_region",
        F.col("p.risk_score").alias("holder_risk_score_2"),
        F.col("p.joined_on").alias("holder_joined_on"),
        "p.holder_name",
    )
)

result = (
    combined_details
    .groupingSets(
        [["holder_id", "status"], ["holder_id"], ["status"], []],
        "holder_id", "status",
    )
    .agg(
        F.grouping("holder_id").alias("is_total_holder_id"),
        F.countDistinct("holder_id").alias("distinct_holder_ids"),
        F.round(F.sum("holder_risk_score_2") / F.nullif(F.sum("premium"), F.lit(0)), 3).alias("holder_risk_score_2_to_premium_ratio"),
        F.count("*").alias("row_count"),
    )
    .select(
        "holder_id",
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "is_total_holder_id",
        "distinct_holder_ids",
        "holder_risk_score_2_to_premium_ratio",
        "row_count",
    )
)
