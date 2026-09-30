from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claimed_amount = Window.orderBy("claimed_amount", "id")
w_policy_id_status = Window.partitionBy("policy_id", "status")
w_all_by_claimed_amount_runnin = Window.orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claimed_amount_seq_product_b = Window.partitionBy("claimed_amount_seq", "product").orderBy("claimed_amount_decile", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claimed_amount_decile_claime = Window.partitionBy("claimed_amount_decile", "claimed_amount_seq")
w_claimed_amount_seq_claimed_a = Window.partitionBy("claimed_amount_seq", "claimed_amount_decile").orderBy("claimed_amount_percent_rank")

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
        "c.approved_amount",
        "p.product",
        "p.premium",
    )
)

metrics = (
    covered
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "approved_amount",
        "product",
        F.ntile(10).over(w_all_by_claimed_amount).alias("claimed_amount_decile"),
        F.sum("claimed_amount").over(w_policy_id_status).alias("policy_id_status_sum_claimed_amount"),
        F.sum("premium").over(w_all_by_claimed_amount_runnin).alias("running_premium"),
        F.round(F.percent_rank().over(w_all_by_claimed_amount), 3).alias("claimed_amount_percent_rank"),
        F.row_number().over(w_all_by_claimed_amount).alias("claimed_amount_seq"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.max("running_premium").over(w_claimed_amount_seq_product_b).alias("max_running_premium_so_far"),
        F.first("claimed_amount_percent_rank", ignorenulls=True).over(w_claimed_amount_seq_product_b).alias("first_known_claimed_amount_percent_rank"),
        F.size(F.collect_set("product").over(w_claimed_amount_decile_claime)).alias("distinct_product_in_claimed_amount_decile_claime"),
        F.round(F.col("claimed_amount_seq") / F.nullif(F.sum("claimed_amount_seq").over(w_claimed_amount_decile_claime), F.lit(0)), 4).alias("claimed_amount_seq_share_of_claimed_amount_decil"),
        F.dense_rank().over(w_claimed_amount_seq_claimed_a).alias("claimed_amount_percent_rank_dense_rank"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
        F.sum("approved_amount").alias("total_claim_approved_amount"),
    )
)

summarized_rows = (
    with_trends.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.avg_claim_approved_amount",
        "c.total_claim_approved_amount",
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "policy_id",
        "claimed_amount",
        "approved_amount",
        "policy_id_status_sum_claimed_amount",
        "running_premium",
        "max_running_premium_so_far",
        "first_known_claimed_amount_percent_rank",
        "distinct_product_in_claimed_amount_decile_claime",
        "claimed_amount_seq_share_of_claimed_amount_decil",
        "claimed_amount_percent_rank_dense_rank",
        "claim_count",
        F.least(F.col("running_premium"), F.col("claim_count")).alias("least_running_premium_claim_count"),
    )
    .orderBy(
        F.col("claimed_amount_percent_rank_dense_rank").asc_nulls_last(),
        "first_known_claimed_amount_percent_rank",
    )
)
