from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_date = Window.orderBy(F.col("claim_date").asc_nulls_last(), "id")
w_product = Window.partitionBy("product")
w_all_by_claim_date_ties = Window.orderBy(F.col("claim_date").asc_nulls_last())

in_window = (
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
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

metrics = (
    in_window
    .select(
        "*",
        (F.col("approved_amount") - F.lag("approved_amount").over(w_all_by_claim_date)).alias("approved_amount_change"),
        F.size(F.collect_set("policy_id").over(w_product)).alias("distinct_policy_id_in_product"),
        F.dense_rank().over(w_all_by_claim_date_ties).alias("claim_date_dense_rank"),
        F.round(F.avg("claimed_amount").over(w_product), 2).alias("product_avg_claimed_amount"),
        F.ntile(4).over(w_all_by_claim_date).alias("claim_date_quartile"),
    )
)

adjuster_id_stats = (
    metrics
    .groupBy("adjuster_id")
    .agg(F.avg("product_avg_claimed_amount").alias("avg_product_avg_claimed_amount"))
)

compared = (
    metrics.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.product_avg_claimed_amount") > F.col("g.avg_product_avg_claimed_amount"))
    .select(
        "s.*",
        "g.avg_product_avg_claimed_amount",
        F.round(F.col("s.product_avg_claimed_amount") - F.col("g.avg_product_avg_claimed_amount"), 2).alias("product_avg_claimed_amount_minus_avg"),
    )
)

result = (
    compared
    .select(
        "id",
        "claim_date",
        "claimed_amount",
        "product",
        "premium",
        "approved_amount_change",
        "distinct_policy_id_in_product",
        "claim_date_dense_rank",
        "product_avg_claimed_amount",
        "product_avg_claimed_amount_minus_avg",
        (F.col("approved_amount_change") - F.col("approved_amount")).alias("approved_amount_change_minus_approved_amount"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
    .orderBy("product_avg_claimed_amount")
)
