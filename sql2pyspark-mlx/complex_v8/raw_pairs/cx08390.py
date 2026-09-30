from pyspark.sql import functions as F

base = (
    df_policyholders
    .filter(F.upper(F.trim(F.col("holder_name"))).like("M%"))
    .select(
        "id",
        "holder_name",
        "region",
        "joined_on",
        "segment",
        F.length(F.trim(F.col("region"))).alias("region_len"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("policy_status_count"),
        F.count("*").alias("policy_count"),
        F.count("*").alias("policy_count_2"),
    )
)

profiled = (
    base.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_status_count"), F.lit(0)).alias("policy_status_count"),
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        F.coalesce(F.col("p.policy_count_2"), F.lit(0)).alias("policy_count_2"),
    )
)

screened = (
    profiled.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.premium") > 2289.0) & (F.col("p.end_date") < F.lit("2026-10-23").cast("date")), "left_anti")
)

totals = (
    screened
    .groupBy("policy_count_2")
    .agg(
        F.max("joined_on").alias("last_joined_on"),
        F.array_sort(F.collect_set("region")).alias("region_list"),
        F.datediff(F.max("joined_on"), F.min("joined_on")).alias("joined_on_span_days"),
        F.countDistinct("region").alias("distinct_regions"),
        F.round(F.avg("region_len"), 2).alias("avg_region_len"),
        F.sum("policy_count").alias("sum_policy_count_check"),
    )
    .filter(F.col("sum_policy_count_check") > 1)
    .drop("sum_policy_count_check")
)

result = (
    totals
    .filter(F.col("policy_count_2") < 6)
    .select(
        "policy_count_2",
        "joined_on_span_days",
        "distinct_regions",
        F.when(F.col("avg_region_len") >= 9.8, "HIGH").when(F.col("avg_region_len") >= 5.0, "MEDIUM").otherwise("LOW").alias("avg_region_len_tier"),
        F.when(F.col("avg_region_len") >= 12.2, "large").when(F.col("avg_region_len") >= 5.0, "medium").otherwise("small").alias("avg_region_len_tier_2"),
    )
    .orderBy(F.col("avg_region_len_tier_2").asc())
)
