from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy("approved_amount", "id").rowsBetween(-1, -1)
w_adjuster_id_by_approved_amou_2 = Window.partitionBy("adjuster_id").orderBy("approved_amount", "id")
w_adjuster_id_by_approved_amou_3 = Window.partitionBy("adjuster_id").orderBy("approved_amount", "id").rowsBetween(-1, Window.currentRow)
w_adjuster_id = Window.partitionBy("adjuster_id")

in_window = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "left",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.adjuster_id",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

adjuster_id_stats = (
    in_window
    .groupBy("adjuster_id")
    .agg(F.percentile_approx("premium", 0.5).alias("median_premium"))
)

outliers = (
    in_window.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.premium") >= F.col("g.median_premium"))
    .select(
        "s.*",
        "g.median_premium",
        F.round(F.col("s.premium") - F.col("g.median_premium"), 2).alias("premium_minus_median"),
    )
)

sequenced = (
    outliers
    .select(
        "*",
        F.round(F.avg("approved_amount").over(w_adjuster_id_by_approved_amou), 2).alias("approved_amount_avg_1_prior"),
        F.lag("premium", 1, 0).over(w_adjuster_id_by_approved_amou_2).alias("prev_premium"),
        F.round(F.avg("premium").over(w_adjuster_id_by_approved_amou_3), 2).alias("premium_avg_2"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id)).alias("distinct_policy_id_in_adjuster_id"),
        F.round(F.col("premium_minus_median") - F.avg("premium_minus_median").over(w_adjuster_id), 2).alias("premium_minus_median_vs_adjuster_id_avg"),
    )
)

with_details_policy = (
    sequenced.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.start_date") < F.lit("2025-02-14").cast("date")), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .filter(F.col("p.start_date").isNotNull())
    .select(
        "s.id",
        "s.policy_id",
        "s.claimed_amount",
        "s.adjuster_id",
        "s.approved_amount",
        "s.product",
        "s.premium",
        "s.median_premium",
        "s.premium_minus_median",
        "s.prev_premium",
        "s.premium_avg_2",
        "s.distinct_policy_id_in_adjuster_id",
        "s.premium_minus_median_vs_adjuster_id_avg",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("po.risk_score").alias("holder_risk_score"),
    )
)

result = (
    with_details_policy
    .select(
        "id",
        F.col("premium_avg_2").alias("m_premium_avg_2"),
        F.col("premium_minus_median_vs_adjuster_id_avg").alias("premium_minus_median_vs_adjuster_id_avg_v"),
        F.col("median_premium").alias("m_median_premium"),
        F.col("holder_risk_score").alias("holder_risk_score_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_premium_avg_2", "premium_minus_median_vs_adjuster_id_avg_v", "m_median_premium", "holder_risk_score_v"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
