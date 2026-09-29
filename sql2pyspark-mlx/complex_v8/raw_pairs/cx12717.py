from pyspark.sql import functions as F

cleaned_holders = (
    df_policyholders
    .filter((F.col("risk_score") > 0.1) & (F.col("joined_on").isNotNull()))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.abs(F.col("risk_score") - 0.6).alias("risk_score_distance"),
    )
)

line_level_policys = (
    cleaned_holders.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.region",
        "s.risk_score",
        "s.joined_on",
        "s.segment",
        "s.risk_score_distance",
        F.col("p.id").alias("policy_id"),
        F.col("p.product").alias("policy_product"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.status").alias("policy_status"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

result = (
    line_level_policys
    .groupingSets(
        [["policy_product", "segment", "policy_status"], ["policy_product"]],
        "policy_product", "segment", "policy_status",
    )
    .agg(
        F.round(100 * F.count(F.when(~F.col("policy_status").isin("LAPSED", "CANCELLED"), 1)) / F.count("*"), 1).alias("pct_policy_status_not_in_lapsed"),
    )
)
