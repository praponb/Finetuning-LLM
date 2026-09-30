from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_adjuster_id_by_cl = Window.partitionBy("claim_type", "adjuster_id").orderBy(F.col("claimed_amount").desc(), "id")
w_reported_at_to_date_adjuster = Window.partitionBy(F.to_date("reported_at"), "adjuster_id").orderBy(F.col("claimed_amount").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_status_adjuster_id = Window.partitionBy("status", "adjuster_id")
w_reported_at_to_date_adjuster_2 = Window.partitionBy(F.to_date("reported_at"), "adjuster_id").orderBy(F.col("claimed_amount").desc_nulls_first(), "id")
w_claim_type_adjuster_id_by_cl_2 = Window.partitionBy("claim_type", "adjuster_id").orderBy(F.col("claimed_amount").desc())
w_status_reported_at_to_date_b = Window.partitionBy("status", F.to_date("reported_at")).orderBy(F.col("reported_at").desc(), "id")
w_claim_type_adjuster_id_by_cl_3 = Window.partitionBy("claim_type", "adjuster_id").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(-3, Window.currentRow)
w_policy_id_status_by_reported = Window.partitionBy("policy_id", "status").orderBy("reported_at", "id").rowsBetween(-3, Window.currentRow)

valid_claims = (
    df_claims
    .filter(F.col("claimed_amount") >= 20436.5)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
    )
)

sequenced = (
    valid_claims
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.row_number().over(w_claim_type_adjuster_id_by_cl).alias("seq_no"),
        F.last("claim_date", ignorenulls=True).over(w_reported_at_to_date_adjuster).alias("last_known_claim_date"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_adjuster_id_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_adjuster_id_policy_id"),
        F.count(F.lit(1)).over(w_status_adjuster_id).alias("rows_in_status_adjuster_id"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_reported_at_to_date_adjuster_2)).alias("claimed_amount_change"),
        F.dense_rank().over(w_claim_type_adjuster_id_by_cl_2).alias("claimed_amount_dense_rank"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_adjuster_id_policy_id), F.lit(0)), 4).alias("approved_amount_share_of_adjuster_id_policy_id"),
        F.rank().over(w_status_reported_at_to_date_b).alias("reported_at_rank"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_id_policy_id), 2).alias("approved_amount_vs_adjuster_id_policy_id_avg"),
        F.sum("claimed_amount").over(w_claim_type_adjuster_id_by_cl_3).alias("claimed_amount_sum_4"),
        F.round(F.avg("approved_amount").over(w_policy_id_status_by_reported), 2).alias("approved_amount_avg_4"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        "seq_no",
        "last_known_claim_date",
        "rows_in_status_adjuster_id",
        "claimed_amount_change",
        "claimed_amount_dense_rank",
        "reported_at_rank",
        "claimed_amount_sum_4",
        "approved_amount_avg_4",
        F.date_trunc("year", F.col("claim_date")).alias("claim_date_year"),
    )
)
