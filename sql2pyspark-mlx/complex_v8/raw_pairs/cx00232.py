from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy("approved_amount", "id")
w_adjuster_id_by_approved_amou_2 = Window.partitionBy("adjuster_id").orderBy("approved_amount")
w_status_by_claim_date_prior_6 = Window.partitionBy("status").orderBy("claim_date", "id").rowsBetween(-6, -1)
w_latest = Window.partitionBy("adjuster_id", "adjuster_team").orderBy(F.col("approved_amount_cume_dist").desc(), F.col("id").desc())

base = (
    df_claims
    .filter(F.col("adjuster_id") >= 2)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.date_format(F.col("reported_at"), "yyyy-MM-dd HH:00").alias("reported_at_label"),
        F.round(F.col("approved_amount") * 0.25, 2).alias("approved_amount_adj"),
    )
)

with_details = (
    base.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (~F.col("p.status").isin("EXPIRED", "LAPSED")), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.status",
        F.col("p.product").alias("policy_product"),
    )
)

ranked = (
    with_details
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        F.ntile(4).over(w_adjuster_id_by_approved_amou).alias("approved_amount_quartile"),
        F.lag("approved_amount", 1, 0).over(w_adjuster_id_by_approved_amou).alias("prev_approved_amount"),
        F.ntile(3).over(w_adjuster_id_by_approved_amou).alias("approved_amount_tercile"),
        F.round(F.cume_dist().over(w_adjuster_id_by_approved_amou), 3).alias("approved_amount_cume_dist"),
        F.round(F.percent_rank().over(w_adjuster_id_by_approved_amou_2), 3).alias("approved_amount_percent_rank"),
        F.sum("approved_amount").over(w_status_by_claim_date_prior_6).alias("approved_amount_sum_6_prior"),
    )
)

with_parent_adjuster = (
    ranked.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.approved_amount_quartile",
        "s.prev_approved_amount",
        "s.approved_amount_tercile",
        "s.approved_amount_cume_dist",
        "s.approved_amount_percent_rank",
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
    )
)

joined_policy = (
    with_parent_adjuster.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.approved_amount_quartile",
        "s.prev_approved_amount",
        "s.approved_amount_tercile",
        "s.approved_amount_cume_dist",
        "s.approved_amount_percent_rank",
        "s.adjuster_team",
        "s.adjuster_name",
        F.col("p.premium").alias("policy_premium"),
    )
)

latest = (
    joined_policy
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

joined_adjuster = (
    latest.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.approved_amount",
        "s.approved_amount_quartile",
        "s.prev_approved_amount",
        "s.approved_amount_cume_dist",
        "s.approved_amount_percent_rank",
        "s.adjuster_name",
        "s.policy_premium",
        F.col("a.level").alias("adjuster_level"),
    )
)

result = (
    joined_adjuster
    .select("id", "adjuster_level", "approved_amount_quartile")
    .unpivot(
        ids=["id"],
        values=["adjuster_level", "approved_amount_quartile"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .orderBy("id", "metric_name")
)
