from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_to_date_adjuster = Window.partitionBy(F.to_date("reported_at"), "adjuster_id")
w_claim_type_adjuster_id_by_cl = Window.partitionBy("claim_type", "adjuster_id").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claimed_amount_full = Window.orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type = Window.partitionBy("claim_type")
w_all_by_reported_at = Window.orderBy(F.col("reported_at").desc_nulls_first(), "id")
w_claim_type_policy_id_by_clai = Window.partitionBy("claim_type", "policy_id").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id = Window.partitionBy("adjuster_id")

cleaned = (
    df_claims
    .filter((F.col("status").isNotNull()) & (F.col("claim_type") == "medical"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "reported_at",
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_reported_at_to_date_adjuster), 2).alias("claimed_amount_vs_reported_at_to_date_adjuster_i"),
        F.first("reported_at").over(w_claim_type_adjuster_id_by_cl).alias("first_reported_at"),
        F.nth_value("reported_at", 2).over(w_all_by_claimed_amount_full).alias("second_reported_at"),
        F.count(F.lit(1)).over(w_reported_at_to_date_adjuster).alias("rows_in_reported_at_to_date_adjuster_id"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_type), 2).alias("claimed_amount_vs_claim_type_avg"),
        F.dense_rank().over(w_all_by_reported_at).alias("reported_at_dense_rank"),
        F.sum("claimed_amount").over(w_claim_type_policy_id_by_clai).alias("running_claimed_amount"),
        F.size(F.collect_set("policy_id").over(w_adjuster_id)).alias("distinct_policy_id_in_adjuster_id"),
    )
)

first_rows = (
    with_windows
    .filter((F.col("reported_at_dense_rank") <= 3) & (F.col("claimed_amount_vs_reported_at_to_date_adjuster_i") >= -15000.0))
    .drop("reported_at_dense_rank")
)

result = (
    first_rows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "reported_at",
        "claim_type_slug",
        "claimed_amount_vs_reported_at_to_date_adjuster_i",
        "first_reported_at",
        "second_reported_at",
        "rows_in_reported_at_to_date_adjuster_id",
        "claimed_amount_vs_claim_type_avg",
        "running_claimed_amount",
    )
    .orderBy(F.col("claim_type").desc())
)
