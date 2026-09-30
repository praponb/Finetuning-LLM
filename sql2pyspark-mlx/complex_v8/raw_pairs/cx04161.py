from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id = Window.partitionBy("policy_id")
w_policy_id_by_premium = Window.partitionBy("policy_id").orderBy("premium", "id")
w_all_by_premium_ties = Window.orderBy("premium")
w_policy_id_by_premium_running = Window.partitionBy("policy_id").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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
        "c.status",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

scored = (
    covered
    .select(
        "*",
        F.trunc(F.col("claim_date"), "MM").alias("claim_date_month_start"),
        F.concat_ws("-", F.col("status"), F.col("product")).alias("status_product_label"),
        F.when(F.col("claimed_amount") >= 32441.2, "large").when(F.col("claimed_amount") >= 18478.3, "medium").otherwise("small").alias("claimed_amount_tier"),
    )
)

qualified = (
    scored.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.status") == "PAID"), "left_semi")
)

windowed = (
    qualified
    .select(
        "id",
        "policy_id",
        "claim_date",
        "status",
        "adjuster_id",
        "claim_date_month_start",
        "claimed_amount_tier",
        F.round(F.col("premium") - F.avg("premium").over(w_policy_id), 2).alias("premium_vs_policy_id_avg"),
        F.lag("claim_date_month_start").over(w_policy_id_by_premium).alias("prev_claim_date_month_start"),
        (F.col("premium") - F.lag("premium").over(w_policy_id_by_premium)).alias("premium_change"),
        F.round(F.percent_rank().over(w_all_by_premium_ties), 3).alias("premium_percent_rank"),
        F.first("claim_date_month_start", ignorenulls=True).over(w_policy_id_by_premium_running).alias("first_known_claim_date_month_start"),
    )
)

result = (
    windowed
    .filter(F.col("prev_claim_date_month_start").isNotNull())
    .select(
        "id",
        "claim_date",
        "status",
        "adjuster_id",
        "claim_date_month_start",
        "claimed_amount_tier",
        "prev_claim_date_month_start",
        "premium_change",
        "first_known_claim_date_month_start",
    )
)
