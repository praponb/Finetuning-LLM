from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id = Window.partitionBy("policy_id")
w_claim_type_by_premium_moving = Window.partitionBy("claim_type").orderBy("premium", "id").rowsBetween(-1, Window.currentRow)
w_policy_id_claim_type_by_appr = Window.partitionBy("policy_id", "claim_type").orderBy("approved_amount")
w_policy_id_claim_type_by_appr_2 = Window.partitionBy("policy_id", "claim_type").orderBy("approved_amount", "id")

covered = (
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
        "c.claim_type",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

with_trends = (
    covered
    .select(
        "*",
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.round(F.avg("claimed_amount").over(w_claim_type_by_premium_moving), 2).alias("claimed_amount_avg_2"),
        F.round(F.cume_dist().over(w_policy_id_claim_type_by_appr), 3).alias("approved_amount_cume_dist"),
        F.rank().over(w_policy_id_claim_type_by_appr).alias("approved_amount_rank"),
        F.row_number().over(w_policy_id_claim_type_by_appr_2).alias("rn"),
    )
)

qualified = (
    with_trends
    .crossJoin(with_trends.agg(F.avg("premium").alias("overall_avg_premium")))
    .filter(F.col("premium") > F.col("overall_avg_premium"))
    .drop("overall_avg_premium")
)

line_level_claims = (
    qualified.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.claimed_amount",
        "s.claim_type",
        "s.approved_amount",
        "s.product",
        "s.premium",
        "s.claimed_amount_avg_2",
        "s.approved_amount_rank",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        F.col("c.reported_at").alias("claim_reported_at"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
    )
)

result = (
    line_level_claims
    .filter(F.col("approved_amount") <= 16404.6)
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "claim_type",
        "premium",
        "approved_amount_rank",
        "claim_id",
        "claim_status",
        F.date_format(F.col("claim_date"), "yyyy-MM-dd").alias("claim_date_label"),
        F.when(F.col("approved_amount_rank") >= 7, "A").when(F.col("approved_amount_rank") >= 4, "B").otherwise("C").alias("approved_amount_rank_tier"),
    )
)
