from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_status_policy_product = Window.partitionBy("policy_status", "policy_product").orderBy("risk_score", "id", "policy_id")
w_segment_region = Window.partitionBy("segment", "region")

base = (
    df_policyholders
    .filter((F.col("joined_on") >= F.lit("2024-07-18").cast("date")) & (F.col("risk_score") >= 0.3))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "segment",
        F.abs(F.col("risk_score") - 0.2).alias("risk_score_distance"),
        F.coalesce(F.col("risk_score"), F.lit(0.0)).alias("risk_score_filled"),
        F.round(F.col("risk_score") * 1.1, 2).alias("risk_score_adj"),
    )
)

line_level_policys = (
    base.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.holder_name",
        "s.region",
        "s.risk_score",
        "s.segment",
        "s.risk_score_adj",
        F.col("p.id").alias("policy_id"),
        F.col("p.product").alias("policy_product"),
        F.col("p.status").alias("policy_status"),
    )
)

ranked = (
    line_level_policys
    .select(
        "*",
        F.lead("risk_score_adj", 2).over(w_policy_status_policy_product).alias("next2_risk_score_adj"),
        F.rank().over(w_policy_status_policy_product).alias("risk_score_rank"),
        F.count(F.lit(1)).over(w_segment_region).alias("rows_in_segment_region"),
    )
)

result = (
    ranked
    .filter(~F.col("policy_product").isin("TRAVEL", "HOME"))
    .select(
        "id",
        "risk_score",
        "segment",
        "risk_score_adj",
        "policy_product",
        "policy_status",
        "rows_in_segment_region",
    )
    .orderBy("segment")
)
