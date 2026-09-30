from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_date = Window.orderBy("claim_date", "id")
w_status_by_claim_date = Window.partitionBy("status").orderBy(F.col("claim_date").desc(), "id")
w_status_adjuster_id_by_claim = Window.partitionBy("status", "adjuster_id").orderBy("claim_date", "id")
w_claim_type = Window.partitionBy("claim_type")
w_claim_type_or_unknown_policy = Window.partitionBy("claim_type_or_unknown", "policy_id").orderBy("claim_date", "id")
w_adjuster_id = Window.partitionBy("adjuster_id")
w_claim_type_or_unknown_policy_2 = Window.partitionBy("claim_type_or_unknown", "policy_id").orderBy("claim_date", "id").rowsBetween(-2, -1)
w_status_adjuster_id_by_claim_2 = Window.partitionBy("status", "adjuster_id").orderBy("claim_date")
w_claim_type_or_unknown_policy_3 = Window.partitionBy("claim_type_or_unknown", "policy_id").orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_claims
    .filter((F.col("claim_type").isin("fire", "collision")) & (F.hour(F.col("reported_at")).between(7, 10)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "reported_at",
        F.abs(F.col("approved_amount") - 4554.8).alias("approved_amount_distance"),
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

sequenced = (
    prepared
    .select(
        "*",
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_all_by_claim_date))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.ntile(3).over(w_status_by_claim_date).alias("claim_date_tercile"),
        F.round(F.percent_rank().over(w_status_adjuster_id_by_claim), 3).alias("claim_date_percent_rank"),
        F.size(F.collect_set("policy_id").over(w_claim_type)).alias("distinct_policy_id_in_claim_type"),
        F.ntile(3).over(w_claim_type_or_unknown_policy).alias("claim_date_tercile_2"),
        F.row_number().over(w_status_by_claim_date).alias("rn"),
        F.round(F.col("approved_amount_distance") - F.avg("approved_amount_distance").over(w_claim_type), 2).alias("approved_amount_distance_vs_claim_type_avg"),
        F.size(F.collect_set("claim_type_or_unknown").over(w_adjuster_id)).alias("distinct_claim_type_or_unknown_in_adjuster_id"),
        F.round(F.avg("approved_amount_distance").over(w_claim_type_or_unknown_policy_2), 2).alias("approved_amount_distance_avg_2_prior"),
        F.size(F.collect_set("status").over(w_adjuster_id)).alias("distinct_status_in_adjuster_id"),
        F.round(F.cume_dist().over(w_status_adjuster_id_by_claim_2), 3).alias("claim_date_cume_dist"),
        F.round(F.avg("approved_amount_distance").over(w_adjuster_id), 2).alias("adjuster_id_avg_approved_amount_distance"),
        F.size(F.collect_set("adjuster_id").over(w_claim_type)).alias("distinct_adjuster_id_in_claim_type"),
        F.nth_value("reported_at", 2).over(w_claim_type_or_unknown_policy_3).alias("second_reported_at"),
        F.max("approved_amount_distance").over(w_adjuster_id).alias("adjuster_id_max_approved_amount_distance"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "status",
        "reported_at",
        "claim_type_or_unknown",
        "minutes_since_prev_reported_at",
        "distinct_policy_id_in_claim_type",
        "claim_date_tercile_2",
        "rn",
        "approved_amount_distance_vs_claim_type_avg",
        "distinct_claim_type_or_unknown_in_adjuster_id",
        "approved_amount_distance_avg_2_prior",
        "distinct_status_in_adjuster_id",
        "claim_date_cume_dist",
        "adjuster_id_avg_approved_amount_distance",
        "distinct_adjuster_id_in_claim_type",
        "second_reported_at",
    )
    .orderBy(F.col("distinct_status_in_adjuster_id").desc(), "policy_id", "id")
    .limit(5)
)
