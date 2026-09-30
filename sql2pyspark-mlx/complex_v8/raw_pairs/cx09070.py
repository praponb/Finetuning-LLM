from pyspark.sql import functions as F

scoped = (
    df_policyholders
    .filter((F.col("segment").isNotNull()) & (F.col("region").isNotNull()))
    .select(
        "id",
        "holder_name",
        "risk_score",
        "joined_on",
        "segment",
        F.concat_ws(" / ", F.col("holder_name"), F.col("segment")).alias("holder_name_segment_label"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.min("end_date").alias("first_policy_end_date"))
)

summarized_rows = (
    scoped.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select("s.*", "p.first_policy_end_date")
)

policy_stats_2 = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.min("premium").alias("min_policy_premium"),
        F.count("*").alias("policy_count"),
    )
)

profiled = (
    summarized_rows.alias("s")
    .join(policy_stats_2.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.min_policy_premium",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

enriched = (
    profiled
    .select(
        "*",
        (F.floor(F.col("min_policy_premium") / 1000) * 1000).alias("min_policy_premium_bucket"),
    )
)

result = (
    enriched
    .cube("policy_count", "segment")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("cnt"),
        F.max("risk_score").alias("max_risk_score"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .select(
        "policy_count",
        F.coalesce(F.col("segment"), F.lit("ALL")).alias("segment"),
        "grouping_level",
        "cnt",
        "max_risk_score",
        "distinct_ids",
    )
    .orderBy(
        F.col("policy_count").asc_nulls_last(),
        F.col("segment").asc_nulls_last(),
    )
)
