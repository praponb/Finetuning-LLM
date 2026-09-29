from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_risk_score_running = Window.orderBy(F.col("risk_score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_risk_score_tier_segment_by_r = Window.partitionBy("risk_score_tier", "segment").orderBy("risk_score_filled", "id")

scoped = (
    df_policyholders
    .filter(F.col("holder_name").isNotNull())
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "segment",
        F.when(F.col("risk_score") >= 0.8, "HIGH").when(F.col("risk_score") >= 0.3, "MEDIUM").otherwise("LOW").alias("risk_score_tier"),
    )
)

enriched = (
    scoped
    .select(
        "*",
        F.concat(F.col("region"), F.lit("#"), F.col("id").cast("string")).alias("region_ref"),
        F.concat_ws(": ", F.col("region"), F.col("segment")).alias("region_segment_label"),
        F.coalesce(F.col("risk_score"), F.lit(0.0)).alias("risk_score_filled"),
    )
)

with_trends = (
    enriched
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "segment",
        "region_ref",
        "region_segment_label",
        "risk_score_filled",
        F.max("risk_score").over(w_all_by_risk_score_running).alias("max_risk_score_so_far"),
        F.lag("risk_score").over(w_risk_score_tier_segment_by_r).alias("prev_risk_score"),
    )
)

exploded_detail = (
    with_trends.alias("s")
    .join(df_policies.alias("p"), F.col("p.holder_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_name",
        "s.risk_score",
        "s.segment",
        "s.region_ref",
        "s.region_segment_label",
        "s.risk_score_filled",
        F.col("p.id").alias("policy_id"),
        F.col("p.product").alias("policy_product"),
        F.col("p.status").alias("policy_status"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

result = (
    exploded_detail
    .groupingSets(
        [["policy_product", "policy_status"], ["policy_product"], []],
        "policy_product", "policy_status",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("policy_status") == "EXPIRED", 1)) / F.count("*"), 1).alias("pct_policy_status_expired"),
    )
    .orderBy(
        F.col("policy_product").asc_nulls_last(),
        F.col("policy_status").asc_nulls_last(),
    )
)
