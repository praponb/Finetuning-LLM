from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_year_policy_id_by = Window.partitionBy(F.year("claim_date"), "policy_id").orderBy(F.col("claim_date").desc(), "id")
w_all_by_approved_amount_full = Window.orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")
w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_claim_type_by_approved_amoun = Window.partitionBy("claim_type").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_year_policy_id_by_2 = Window.partitionBy(F.year("claim_date"), "policy_id").orderBy(F.col("claim_date").desc(), "id").rowsBetween(-2, Window.currentRow)

recent_claims = (
    df_claims
    .filter((F.month(F.col("reported_at")).isin(1, 2)) & (F.col("status") == "OPEN"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
    )
)

sequenced = (
    recent_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "reported_at",
        F.lead("reported_at").over(w_claim_date_year_policy_id_by).alias("next_reported_at"),
        F.nth_value("approved_amount", 2).over(w_all_by_approved_amount_full).alias("second_approved_amount"),
        F.lead("claim_date").over(w_all_by_approved_amount).alias("next_claim_date"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_id_policy_id), 2).alias("approved_amount_vs_adjuster_id_policy_id_avg"),
        F.lag("claim_date").over(w_all_by_approved_amount).alias("prev_claim_date"),
        F.last("approved_amount").over(w_claim_type_by_approved_amoun).alias("last_approved_amount"),
        F.ntile(4).over(w_claim_date_year_policy_id_by).alias("claim_date_quartile"),
        F.sum("approved_amount").over(w_claim_date_year_policy_id_by_2).alias("approved_amount_sum_3"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "reported_at",
        "next_reported_at",
        "second_approved_amount",
        "next_claim_date",
        "approved_amount_vs_adjuster_id_policy_id_avg",
        "prev_claim_date",
        "last_approved_amount",
        "claim_date_quartile",
        "approved_amount_sum_3",
        F.coalesce(F.col("last_approved_amount"), F.lit(0.0)).alias("last_approved_amount_filled"),
    )
    .orderBy(F.col("claim_date_quartile").asc_nulls_last(), "claim_type", "id")
    .limit(10)
)
