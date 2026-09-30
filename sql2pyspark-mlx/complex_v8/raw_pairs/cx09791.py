from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_by_premium = Window.partitionBy("claim_type").orderBy("premium", "id")
w_claim_date_month_claim_type = Window.partitionBy(F.month("claim_date"), "claim_type")

covered = (
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
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

with_windows = (
    covered
    .select(
        "*",
        F.round(F.percent_rank().over(w_claim_type_by_premium), 3).alias("premium_percent_rank"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_claim_date_month_claim_type), F.lit(0)), 4).alias("claimed_amount_share_of_claim_date_month_claim_t"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_date_month_claim_type), 2).alias("claimed_amount_vs_claim_date_month_claim_type_av"),
    )
)

result = (
    with_windows
    .filter(F.col("product").isin("LIFE", "TRAVEL", "HOME"))
    .select(
        "id",
        "policy_id",
        "claim_date",
        "status",
        "claim_type",
        "product",
        "premium_percent_rank",
        "claimed_amount_share_of_claim_date_month_claim_t",
        "claimed_amount_vs_claim_date_month_claim_type_av",
        F.date_trunc("week", F.col("claim_date")).alias("claim_date_week"),
    )
    .orderBy("status")
)
