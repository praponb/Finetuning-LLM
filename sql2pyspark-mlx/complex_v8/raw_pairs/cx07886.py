from pyspark.sql import functions as F

prepared = (
    df_policies
    .filter((F.col("premium") < 3719.7) & (F.col("product").isin("HEALTH", "HOME", "LIFE")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_trunc("month", F.col("end_date")).alias("end_date_month"),
    )
)

with_activity = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.joined_on") < F.lit("2025-02-22").cast("date")) & (~F.col("p.region").isin("Central", "South")), "left_anti")
)

combined_details_holder = (
    with_activity.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date_month",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

enriched = (
    combined_details_holder.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.holder_risk_score",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score_2"),
        F.col("p.region").alias("holder_region"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.max("start_date").alias("last_policy_start_date"),
        F.count("*").alias("policy_count"),
        F.count("*").alias("policy_count_2"),
    )
)

with_stats = (
    enriched.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.last_policy_start_date",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        F.coalesce(F.col("p.policy_count_2"), F.lit(0)).alias("policy_count_2"),
    )
)

with_details_holder = (
    with_stats.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.holder_segment",
        "s.holder_risk_score_2",
        "s.holder_region",
        "s.last_policy_start_date",
        "s.policy_count",
        "s.policy_count_2",
        F.col("p.segment").alias("holder_segment_2"),
        "p.holder_name",
    )
)

tagged = (
    with_details_holder
    .filter(F.col("policy_count_2") <= 6)
    .withColumn("holder_risk_score_2_adj", F.round(F.col("holder_risk_score_2") * 1.5, 2))
)

result = (
    tagged
    .select(
        "id",
        F.col("policy_count").alias("policy_count_v"),
        F.col("policy_count_2").alias("policy_count_2_v"),
    )
    .unpivot(
        ids=["id"],
        values=["policy_count_v", "policy_count_2_v"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "field")
)
