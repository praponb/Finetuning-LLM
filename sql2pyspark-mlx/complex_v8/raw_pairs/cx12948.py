from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_by_policy_end_date = Window.partitionBy("policy_id").orderBy("policy_end_date", "id").rowsBetween(-6, Window.currentRow)
w_policy_id_by_policy_end_date_2 = Window.partitionBy("policy_id").orderBy("policy_end_date", "id")
w_reported_at_to_date_status = Window.partitionBy(F.to_date("reported_at"), "status")
w_claim_type_by_approved_amoun = Window.partitionBy("claim_type").orderBy("approved_amount", "id")
w_policy_id_by_policy_end_date_3 = Window.partitionBy("policy_id").orderBy("policy_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type_by_approved_amoun_2 = Window.partitionBy("claim_type").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type = Window.partitionBy("claim_type")
w_claim_type_adjuster_id_by_cl = Window.partitionBy("claim_type", "adjuster_id").orderBy("claim_type_len")
w_claim_type_len_by_approved_a = Window.partitionBy("claim_type_len").orderBy(F.col("approved_amount_percent_rank").desc(), "id")

recent = (
    df_claims
    .filter(F.col("approved_amount") > 0.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("claim_type_ref"),
    )
)

with_details = (
    recent.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.claim_type",
        "s.approved_amount",
        "s.status",
        "s.reported_at",
        "s.claim_type_len",
        "s.claim_type_ref",
        F.col("p.end_date").alias("policy_end_date"),
    )
)

ranked = (
    with_details
    .select(
        "*",
        F.sum("approved_amount").over(w_policy_id_by_policy_end_date).alias("approved_amount_sum_7"),
        F.row_number().over(w_policy_id_by_policy_end_date_2).alias("rn"),
        F.count(F.lit(1)).over(w_reported_at_to_date_status).alias("rows_in_reported_at_to_date_status"),
        F.lag("policy_end_date").over(w_policy_id_by_policy_end_date_2).alias("prev_policy_end_date"),
        F.sum("approved_amount").over(w_reported_at_to_date_status).alias("reported_at_to_date_status_sum_approved_amount"),
        F.round(F.percent_rank().over(w_claim_type_by_approved_amoun), 3).alias("approved_amount_percent_rank"),
        F.last("approved_amount").over(w_policy_id_by_policy_end_date_3).alias("last_approved_amount"),
        F.first("approved_amount").over(w_claim_type_by_approved_amoun_2).alias("first_approved_amount"),
    )
)

first_rows = (
    ranked
    .filter((F.col("rn") <= 2) & (F.col("prev_policy_end_date").isNotNull()))
    .drop("rn")
)

with_windows = (
    first_rows
    .select(
        "*",
        F.size(F.collect_set("status").over(w_claim_type)).alias("distinct_status_in_claim_type"),
        F.max("first_approved_amount").over(w_claim_type).alias("claim_type_max_first_approved_amount"),
        F.round(F.percent_rank().over(w_claim_type_adjuster_id_by_cl), 3).alias("claim_type_len_percent_rank"),
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_claim_type_len_by_approved_a))) / 60, 1).alias("minutes_since_prev_reported_at"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "status",
        "reported_at",
        "claim_type_len",
        "claim_type_ref",
        "policy_end_date",
        "approved_amount_sum_7",
        "rows_in_reported_at_to_date_status",
        "prev_policy_end_date",
        "reported_at_to_date_status_sum_approved_amount",
        "last_approved_amount",
        "claim_type_len_percent_rank",
        "minutes_since_prev_reported_at",
        F.when(F.col("approved_amount") >= 20880.4, "HIGH").when(F.col("approved_amount") >= 799.5, "MEDIUM").otherwise("LOW").alias("approved_amount_tier"),
    )
)
