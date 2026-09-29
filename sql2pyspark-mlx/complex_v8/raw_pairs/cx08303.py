from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month_by_claimed = Window.partitionBy(F.date_trunc("month", "claim_date")).orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_month_product = Window.partitionBy(F.date_trunc("month", "claim_date"), "product")

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
        "c.adjuster_id",
        "c.claim_type",
        "p.product",
        "p.premium",
    )
)

without_matches = (
    covered.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.year(F.col("p.end_date")) == 2026) & (F.col("p.premium") <= 4417.0), "left_semi")
)

filtered = (
    without_matches.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
    .filter(F.col("premium") < 4678.8)
)

ranked = (
    filtered
    .select(
        "*",
        F.last("claimed_amount").over(w_claim_date_month_by_claimed).alias("last_claimed_amount"),
        F.count(F.lit(1)).over(w_claim_date_month_product).alias("rows_in_claim_date_month_product"),
        F.min("claimed_amount").over(w_claim_date_month_product).alias("claim_date_month_product_min_claimed_amount"),
    )
)

result = (
    ranked
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "adjuster_id",
        "product",
        "last_claimed_amount",
        "rows_in_claim_date_month_product",
        "claim_date_month_product_min_claimed_amount",
        F.date_trunc("year", F.col("claim_date")).alias("claim_date_year"),
    )
    .orderBy(
        F.col("claim_date_month_product_min_claimed_amount").asc(),
        F.col("last_claimed_amount").desc(),
    )
)
