from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_holder_id_by_approved = Window.partitionBy("policy_holder_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_status = Window.partitionBy("policy_id", "status")
w_policy_holder_id_by_approved_2 = Window.partitionBy("policy_holder_id").orderBy("approved_amount", "id")
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_reported_at_year_by_policy_e = Window.partitionBy(F.year("reported_at")).orderBy("policy_end_date")

cleaned = (
    df_claims
    .filter((F.col("claim_type") != "delay") & (F.col("claim_date") >= F.lit("2024-04-07").cast("date")))
)

with_parent_policy = (
    cleaned.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.start_date") < F.lit("2025-08-03").cast("date")), "inner")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.col("p.holder_id").alias("policy_holder_id"),
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.product").alias("policy_product"),
        "a.adjuster_name",
    )
)

sequenced = (
    with_parent_policy
    .select(
        "*",
        F.nth_value("reported_at", 2).over(w_policy_holder_id_by_approved).alias("second_reported_at"),
        F.size(F.collect_set("adjuster_id").over(w_policy_id_status)).alias("distinct_adjuster_id_in_policy_id_status"),
        F.lag("claim_date").over(w_policy_holder_id_by_approved_2).alias("prev_claim_date"),
        F.round(F.percent_rank().over(w_all_by_reported_at), 3).alias("reported_at_percent_rank"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id_status), F.lit(0)), 4).alias("approved_amount_share_of_policy_id_status"),
        F.max("claimed_amount").over(w_policy_id_status).alias("policy_id_status_max_claimed_amount"),
        F.row_number().over(w_all_by_reported_at).alias("seq_no"),
        F.round(F.cume_dist().over(w_reported_at_year_by_policy_e), 3).alias("policy_end_date_cume_dist"),
        F.lag("approved_amount", 1, 0).over(w_all_by_reported_at).alias("prev_approved_amount"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "reported_at",
        "policy_holder_id",
        "policy_end_date",
        "policy_product",
        "adjuster_name",
        "second_reported_at",
        "distinct_adjuster_id_in_policy_id_status",
        "prev_claim_date",
        "reported_at_percent_rank",
        "approved_amount_share_of_policy_id_status",
        "policy_id_status_max_claimed_amount",
        "seq_no",
        "policy_end_date_cume_dist",
        "prev_approved_amount",
    )
    .orderBy(F.col("id").desc(), F.col("claim_type").desc())
)
