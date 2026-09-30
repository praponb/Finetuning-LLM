from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("segment_or_unknown").orderBy(F.col("policy_count").desc(), F.col("id").desc())
w_dedup = Window.partitionBy("segment_or_unknown").orderBy(F.col("avg_policy_premium").desc(), F.col("id").desc())

base_holders = (
    df_policyholders
    .filter((F.year(F.col("joined_on")) == 2024) & (F.col("region") == "South"))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
    )
)

enriched = (
    base_holders
    .filter(F.col("segment") != "SME")
    .select(
        "*",
        F.length(F.trim(F.col("holder_name"))).alias("holder_name_len"),
        F.abs(F.col("risk_score") - 0.3).alias("risk_score_distance"),
    )
)

labeled = (
    enriched
    .select(
        "*",
        F.coalesce(F.col("segment"), F.lit("UNKNOWN")).alias("segment_or_unknown"),
        F.dayofweek(F.col("joined_on")).alias("joined_on_dayofweek"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.round(F.avg("premium"), 2).alias("avg_policy_premium"),
        F.count("*").alias("policy_count"),
    )
)

with_stats = (
    labeled.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.avg_policy_premium",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

latest_per_key = (
    with_stats
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

unique_rows = (
    latest_per_key
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    unique_rows
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "holder_name_len",
        "segment_or_unknown",
        "policy_count",
        F.round(F.col("holder_name_len") / F.nullif(F.col("avg_policy_premium"), F.lit(0)), 3).alias("holder_name_len_per_avg_policy_premium"),
    )
    .orderBy("id", F.col("policy_count").desc())
    .limit(10)
)
