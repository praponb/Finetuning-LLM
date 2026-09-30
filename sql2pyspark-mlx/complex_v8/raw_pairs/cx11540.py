from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_claim_type_by_clai = Window.partitionBy("policy_id", "claim_type").orderBy("claimed_amount", "id")
w_policy_id_claim_type_by_clai_2 = Window.partitionBy("policy_id", "claim_type").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product = Window.partitionBy("product")
w_policy_id_claim_type_by_clai_3 = Window.partitionBy("policy_id", "claim_type").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium = Window.orderBy("premium", "id")

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
        "c.claim_type",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

ranked = (
    covered
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "claim_type",
        "adjuster_id",
        "product",
        "premium",
        F.row_number().over(w_policy_id_claim_type_by_clai).alias("seq_no"),
        F.max("claimed_amount").over(w_policy_id_claim_type_by_clai_2).alias("max_claimed_amount_so_far"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_product), 2).alias("claimed_amount_vs_product_avg"),
        F.nth_value("claimed_amount", 2).over(w_policy_id_claim_type_by_clai_3).alias("second_claimed_amount"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_all_by_premium)).alias("days_since_prev_claim_date"),
        F.round(F.cume_dist().over(w_policy_id_claim_type_by_clai), 3).alias("claimed_amount_cume_dist"),
    )
)

with_details = (
    ranked.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("a.level").between(1, 3))
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.product",
        "s.seq_no",
        "s.max_claimed_amount_so_far",
        "s.claimed_amount_vs_product_avg",
        "s.second_claimed_amount",
        "s.days_since_prev_claim_date",
        "s.claimed_amount_cume_dist",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
    )
)

result = (
    with_details
    .rollup("product", "adjuster_id")
    .agg(
        F.grouping("product").alias("is_total_product"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("max_claimed_amount_so_far").alias("total_max_claimed_amount_so_far"),
    )
    .select(
        F.coalesce(F.col("product"), F.lit("ALL")).alias("product"),
        "adjuster_id",
        "is_total_product",
        "distinct_ids",
        "total_max_claimed_amount_so_far",
    )
)
