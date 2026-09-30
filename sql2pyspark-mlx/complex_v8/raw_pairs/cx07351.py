from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_claim_type_adjuster_id_by_re = Window.partitionBy("claim_type", "adjuster_id").orderBy("reported_at", "id")
w_all_by_approved_amount_fille = Window.orderBy(F.col("approved_amount_filled_change").desc(), "id")
w_all_by_approved_amount_fille_2 = Window.orderBy(F.col("approved_amount_filled_change").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_claims = (
    df_claims
    .filter((F.col("approved_amount") >= 0.0) & (F.col("status").isin("APPROVED", "REJECTED")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
    )
)

ranked = (
    scoped_claims
    .select(
        "*",
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status), 2).alias("claimed_amount_vs_status_avg"),
        (F.col("approved_amount_filled") - F.lag("approved_amount_filled").over(w_claim_type_adjuster_id_by_re)).alias("approved_amount_filled_change"),
        F.lead("reported_at", 2).over(w_claim_type_adjuster_id_by_re).alias("next2_reported_at"),
    )
)

windowed = (
    ranked
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount_filled_change",
        "next2_reported_at",
        F.rank().over(w_all_by_approved_amount_fille).alias("approved_amount_filled_change_rank"),
        F.count(F.lit(1)).over(w_status).alias("rows_in_status"),
        F.last("approved_amount_filled").over(w_all_by_approved_amount_fille_2).alias("last_approved_amount_filled"),
        F.size(F.collect_set("adjuster_id").over(w_status)).alias("distinct_adjuster_id_in_status"),
    )
)

result = (
    windowed
    .groupingSets(
        [["distinct_adjuster_id_in_status", "approved_amount_filled_change_rank", "claim_type"], ["distinct_adjuster_id_in_status"], ["approved_amount_filled_change_rank"], []],
        "distinct_adjuster_id_in_status", "approved_amount_filled_change_rank", "claim_type",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("claim_type").alias("distinct_claim_types"),
    )
    .select(
        "distinct_adjuster_id_in_status",
        "approved_amount_filled_change_rank",
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        "gid",
        "distinct_claim_types",
    )
    .orderBy(
        F.col("distinct_adjuster_id_in_status").asc_nulls_last(),
        F.col("approved_amount_filled_change_rank").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
    )
)
