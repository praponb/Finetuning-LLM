from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_premium_running = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_premium_ties = Window.orderBy("premium")
w_policy_id_by_claim_date = Window.partitionBy("policy_id").orderBy("claim_date", "id")
w_policy_id_by_claim_date_prio = Window.partitionBy("policy_id").orderBy("claim_date", "id").rowsBetween(-2, -1)
w_product_policy_id = Window.partitionBy("product", "policy_id")

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
        "c.status",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

without_matches = (
    in_window.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.month(F.col("c.reported_at")).isin(1, 2)) & (F.col("c.adjuster_id").isNotNull()), "left_semi")
)

sequenced = (
    without_matches
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "status",
        "product",
        "premium",
        F.last("claim_date", ignorenulls=True).over(w_all_by_premium_running).alias("last_known_claim_date"),
        F.round(F.percent_rank().over(w_all_by_premium_ties), 3).alias("premium_percent_rank"),
        F.rank().over(w_policy_id_by_claim_date).alias("claim_date_rank"),
        F.round(F.avg("claimed_amount").over(w_policy_id_by_claim_date_prio), 2).alias("claimed_amount_avg_2_prior"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_product_policy_id), 2).alias("claimed_amount_vs_product_policy_id_avg"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "policy_id",
        "claim_date",
        "status",
        "product",
        "premium",
        "premium_percent_rank",
        "claim_date_rank",
        "claimed_amount_avg_2_prior",
        "claimed_amount_vs_product_policy_id_avg",
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
        F.when(F.col("product").isin("TRAVEL", "LIFE"), "group_a").when(F.col("product") == "HOME", "group_b").otherwise("other").alias("product_group"),
    )
    .orderBy("premium", "claim_date_rank")
)
