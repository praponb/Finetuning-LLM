from pyspark.sql import functions as F

scoped_holders = (
    df_policyholders
    .filter(F.col("risk_score") > 0.1)
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.substring(F.trim(F.col("region")), 1, 2).alias("region_prefix"),
        F.when(F.col("region").isin("Central", "East"), "group_a").when(F.col("region") == "South", "group_b").otherwise("other").alias("region_group"),
    )
)

tagged = (
    scoped_holders
    .select(
        "id",
        "region",
        "risk_score",
        "joined_on",
        "region_prefix",
        "region_group",
        F.date_format(F.col("joined_on"), "yyyy-MM").alias("joined_on_label"),
        F.abs(F.col("risk_score") - 0.3).alias("risk_score_distance"),
    )
)

detail_rows = (
    tagged.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.risk_score",
        "s.joined_on",
        "s.region_prefix",
        "s.region_group",
        "s.joined_on_label",
        F.col("p.id").alias("policy_id"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "risk_score",
        "region_prefix",
        "region_group",
        "joined_on_label",
        "policy_premium",
        "policy_end_date",
    )
    .orderBy("joined_on_label")
)
