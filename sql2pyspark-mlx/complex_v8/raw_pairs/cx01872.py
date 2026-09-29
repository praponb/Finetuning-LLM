from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_premium = Window.orderBy("premium", "id")

matched = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p"),
        (F.col("p.id") == F.col("c.policy_id"))
        & F.col("c.claim_date").between(F.col("p.start_date"), F.coalesce(F.col("p.end_date"), F.lit("2099-12-31").cast("date"))),
        "inner",
    )
    .select(
        "c.id",
        "c.policy_id",
        "c.claim_date",
        "c.claimed_amount",
        "c.status",
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

with_trends = (
    matched
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "premium",
        F.lag("approved_amount", 1, 0).over(w_all_by_premium).alias("prev_approved_amount"),
        F.lag("claim_date").over(w_all_by_premium).alias("prev_claim_date"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_premium)).alias("days_since_prev_claim_date"),
    )
)

qualified = (
    with_trends.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.approved_amount") <= 14668.0) & (F.col("c.adjuster_id").isNotNull()), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "prev_approved_amount",
        "prev_claim_date",
        "days_since_prev_claim_date",
        F.round(F.col("premium") * 1.5, 2).alias("premium_adj"),
    )
    .orderBy("claim_date", "premium_adj")
)
