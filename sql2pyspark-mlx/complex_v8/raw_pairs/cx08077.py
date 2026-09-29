from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_claim_id = Window.partitionBy("adjuster_id", "claim_id")
w_all_by_claim_approved_amount = Window.orderBy(F.col("claim_approved_amount").desc(), "id", "claim_id")
w_all_by_claim_approved_amount_2 = Window.orderBy(F.col("claim_approved_amount").desc())

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
        "c.approved_amount",
        "c.adjuster_id",
        "p.product",
        "p.premium",
    )
)

detail_rows = (
    in_window.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.adjuster_id",
        "s.product",
        "s.premium",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        "c.claim_type",
    )
)

metrics = (
    detail_rows
    .select(
        "*",
        F.size(F.collect_set("product").over(w_adjuster_id_claim_id)).alias("distinct_product_in_adjuster_id_claim_id"),
        F.dense_rank().over(w_all_by_claim_approved_amount).alias("claim_approved_amount_dense_rank"),
        F.round(F.col("claim_approved_amount") / F.nullif(F.sum("claim_approved_amount").over(w_adjuster_id_claim_id), F.lit(0)), 4).alias("claim_approved_amount_share_of_adjuster_id_claim"),
        F.rank().over(w_all_by_claim_approved_amount_2).alias("claim_approved_amount_rank"),
        F.dense_rank().over(w_all_by_claim_approved_amount_2).alias("claim_approved_amount_dense_rank_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "policy_id",
        "claim_date",
        "premium",
        "claim_approved_amount",
        "claim_type",
        "claim_approved_amount_share_of_adjuster_id_claim",
        "claim_approved_amount_rank",
        "claim_approved_amount_dense_rank_2",
        F.when(F.col("product").isin("HOME", "TRAVEL"), "group_a").when(F.col("product") == "AUTO", "group_b").otherwise("other").alias("product_group"),
    )
    .orderBy(F.col("claim_approved_amount_dense_rank_2").asc())
)
