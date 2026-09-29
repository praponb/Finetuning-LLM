from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("segment_or_unknown").orderBy(F.col("risk_score").desc(), F.col("id").desc())

cleaned_holders = (
    df_policyholders
    .select(
        "id",
        "holder_name",
        "risk_score",
        "joined_on",
        F.concat(F.col("segment"), F.lit("#"), F.col("id").cast("string")).alias("segment_ref"),
        F.coalesce(F.col("segment"), F.lit("UNKNOWN")).alias("segment_or_unknown"),
        F.coalesce(F.col("risk_score"), F.lit(0.0)).alias("risk_score_filled"),
    )
)

latest = (
    cleaned_holders
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_children_policys = (
    latest.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_name",
        "s.risk_score",
        "s.segment_ref",
        F.col("p.id").alias("policy_id"),
        F.col("p.product").alias("policy_product"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.status").alias("policy_status"),
    )
)

combined_details = (
    with_children_policys.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.start_date") >= F.lit("2024-07-19").cast("date")), "left")
    .select(
        "s.id",
        "s.risk_score",
        "s.segment_ref",
        "s.policy_id",
        "s.policy_product",
        "s.policy_end_date",
        "s.policy_start_date",
        "s.policy_status",
        F.col("p.start_date").alias("policy_start_date_2"),
        F.col("p.product").alias("policy_product_2"),
    )
)

line_level = (
    combined_details.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.policy_product",
        "s.policy_end_date",
        "s.policy_start_date",
        "s.policy_status",
        F.col("p.id").alias("policy_id_2"),
        F.col("p.end_date").alias("policy_end_date_2"),
        F.col("p.product").alias("policy_product_3"),
        F.col("p.status").alias("policy_status_2"),
        F.col("p.start_date").alias("policy_start_date_3"),
    )
)

summary = (
    line_level
    .groupBy("policy_id", "policy_product_3")
    .agg(
        F.round(100 * F.count(F.when(F.col("policy_status") != "CANCELLED", 1)) / F.count("*"), 1).alias("pct_policy_status_cancelled"),
        F.countDistinct("policy_id_2").alias("distinct_policy_id_2s"),
        F.min("policy_end_date").alias("first_policy_end_date"),
        F.count("*").alias("row_count"),
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.countDistinct("policy_product_3").alias("distinct_policy_product_3s"),
    )
    .filter(F.col("row_count") >= 2)
)

combined_details_policy = (
    summary.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.policy_id",
        "s.policy_product_3",
        "s.pct_policy_status_cancelled",
        "s.distinct_policy_id_2s",
        "s.first_policy_end_date",
        "s.distinct_policy_ids",
        "s.distinct_policy_product_3s",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

result = (
    combined_details_policy
    .select(
        "policy_id",
        "policy_product_3",
        F.col("policy_premium").alias("policy_premium_v"),
        F.col("pct_policy_status_cancelled").alias("m_pct_policy_status_cancelled"),
    )
    .unpivot(
        ids=["policy_id", "policy_product_3"],
        values=["policy_premium_v", "m_pct_policy_status_cancelled"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
