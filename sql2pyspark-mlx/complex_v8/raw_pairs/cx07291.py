from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_product_status = Window.partitionBy("product", "status")
w_status_by_total_claim_claime = Window.partitionBy("status").orderBy("total_claim_claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_total_claim_claime_2 = Window.partitionBy("status").orderBy("total_claim_claimed_amount")

scoped_policys = (
    df_policies
    .filter(F.col("start_date") < F.lit("2025-05-28").cast("date"))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "status",
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("claim_date").alias("last_claim_date"),
        F.sum("claimed_amount").alias("total_claim_claimed_amount"),
    )
)

with_counts = (
    scoped_policys.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.last_claim_date", "c.total_claim_claimed_amount")
)

metrics = (
    with_counts
    .select(
        "id",
        "start_date",
        "status",
        "total_claim_claimed_amount",
        F.min("total_claim_claimed_amount").over(w_product_status).alias("product_status_min_total_claim_claimed_amount"),
        F.size(F.collect_set("holder_id").over(w_product_status)).alias("distinct_holder_id_in_product_status"),
        F.last("total_claim_claimed_amount").over(w_status_by_total_claim_claime).alias("last_total_claim_claimed_amount"),
        F.rank().over(w_status_by_total_claim_claime_2).alias("total_claim_claimed_amount_rank"),
        F.round(F.percent_rank().over(w_status_by_total_claim_claime_2), 3).alias("total_claim_claimed_amount_percent_rank"),
    )
)

without_matches = (
    metrics.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left_semi")
)

result = (
    without_matches
    .groupingSets(
        [["total_claim_claimed_amount_rank", "distinct_holder_id_in_product_status", "status"], ["total_claim_claimed_amount_rank"], ["distinct_holder_id_in_product_status"], []],
        "total_claim_claimed_amount_rank", "distinct_holder_id_in_product_status", "status",
    )
    .agg(
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.max("product_status_min_total_claim_claimed_amount").alias("max_product_status_min_total_claim_claimed_amoun"),
        F.max("distinct_holder_id_in_product_status").alias("max_distinct_holder_id_in_product_status"),
    )
    .select(
        "total_claim_claimed_amount_rank",
        "distinct_holder_id_in_product_status",
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "status_list",
        "max_product_status_min_total_claim_claimed_amoun",
        "max_distinct_holder_id_in_product_status",
    )
    .orderBy(
        F.col("total_claim_claimed_amount_rank").asc_nulls_last(),
        F.col("distinct_holder_id_in_product_status").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
)
