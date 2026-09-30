from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_premium_ties = Window.orderBy("premium")
w_product_or_unknown = Window.partitionBy("product_or_unknown")
w_status_policy_id_by_claim_da = Window.partitionBy("status", "policy_id").orderBy(F.col("claim_date").desc())
w_status_claim_date_rank = Window.partitionBy("status", "claim_date_rank")
w_product_or_unknown_by_premiu = Window.partitionBy("product_or_unknown").orderBy("premium", "id").rowsBetween(-3, Window.currentRow)
w_product_or_unknown_by_premiu_2 = Window.partitionBy("product_or_unknown").orderBy("premium")

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
        "c.adjuster_id",
        "c.status",
        "p.product",
        "p.premium",
    )
)

labeled = (
    covered
    .filter(F.col("claimed_amount") >= 5774.6)
    .select(
        "*",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.when(F.col("product").isin("AUTO", "LIFE"), "group_a").when(F.col("product") == "TRAVEL", "group_b").otherwise("other").alias("product_group"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

sequenced = (
    labeled
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_premium_ties), 3).alias("premium_percent_rank"),
        F.min("premium").over(w_product_or_unknown).alias("product_or_unknown_min_premium"),
        F.rank().over(w_status_policy_id_by_claim_da).alias("claim_date_rank"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.round(F.col("claim_date_rank") - F.avg("claim_date_rank").over(w_status_claim_date_rank), 2).alias("claim_date_rank_vs_status_claim_date_rank_avg"),
        F.round(F.avg("premium_percent_rank").over(w_product_or_unknown_by_premiu), 2).alias("premium_percent_rank_avg_4"),
        F.min("claimed_amount").over(w_status_claim_date_rank).alias("status_claim_date_rank_min_claimed_amount"),
        F.rank().over(w_product_or_unknown_by_premiu_2).alias("premium_rank"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "adjuster_id",
        "product",
        "premium",
        "product_or_unknown",
        "product_group",
        "status_len",
        "product_or_unknown_min_premium",
        "claim_date_rank",
        "premium_percent_rank_avg_4",
        "status_claim_date_rank_min_claimed_amount",
        "premium_rank",
        (F.col("premium_percent_rank") - F.col("premium")).alias("premium_percent_rank_minus_premium"),
        F.date_trunc("week", F.col("claim_date")).alias("claim_date_week"),
    )
    .orderBy(F.col("product_or_unknown_min_premium").desc(), "claim_date_rank")
)
