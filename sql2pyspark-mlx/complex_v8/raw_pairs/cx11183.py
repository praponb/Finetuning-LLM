from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_policy_id = Window.partitionBy("adjuster_id", "policy_id")
w_claim_date_month_by_approved = Window.partitionBy(F.date_trunc("month", "claim_date")).orderBy("approved_amount", "id").rowsBetween(-1, -1)
w_policy_id_adjuster_id_by_app = Window.partitionBy("policy_id", "adjuster_id").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_year_status_by_cl = Window.partitionBy(F.year("claim_date"), "status").orderBy("claim_date", "id").rowsBetween(-2, Window.currentRow)
w_claim_date_year_status_by_cl_2 = Window.partitionBy(F.year("claim_date"), "status").orderBy("claim_date", "id")
w_policy_id_adjuster_id_by_app_2 = Window.partitionBy("policy_id", "adjuster_id").orderBy("approved_amount", "id")
w_claim_type_claim_date_seq = Window.partitionBy("claim_type", "claim_date_seq")
w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy(F.col("approved_amount").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_approved_amount_2 = Window.partitionBy("policy_id").orderBy(F.col("approved_amount").asc_nulls_last())

base_claims = (
    df_claims
    .filter((F.col("claim_date") >= F.lit("2024-07-22").cast("date")) | (F.col("approved_amount") <= 14993.0))
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
    base_claims
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "reported_at",
        F.round(F.avg("approved_amount").over(w_adjuster_id_policy_id), 2).alias("adjuster_id_policy_id_avg_approved_amount"),
        F.round(F.avg("approved_amount").over(w_claim_date_month_by_approved), 2).alias("approved_amount_avg_1_prior"),
        F.first("claim_date").over(w_policy_id_adjuster_id_by_app).alias("first_claim_date"),
        F.max("claimed_amount").over(w_claim_date_year_status_by_cl).alias("claimed_amount_max_3"),
        F.row_number().over(w_claim_date_year_status_by_cl_2).alias("claim_date_seq"),
        (F.col("approved_amount") - F.lag("approved_amount").over(w_claim_date_year_status_by_cl_2)).alias("approved_amount_change"),
        F.lag("claim_date").over(w_policy_id_adjuster_id_by_app_2).alias("prev_claim_date"),
        F.min("approved_amount").over(w_adjuster_id_policy_id).alias("adjuster_id_policy_id_min_approved_amount"),
    )
)

enriched = (
    sequenced
    .filter(F.col("claim_type") == "delay")
    .select(
        "*",
        F.initcap(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.round(F.col("claimed_amount_max_3") * 1.5, 2).alias("claimed_amount_max_3_adj"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "policy_id",
        "claim_date",
        "approved_amount",
        "reported_at",
        "adjuster_id_policy_id_avg_approved_amount",
        "approved_amount_avg_1_prior",
        "first_claim_date",
        "claim_date_seq",
        "approved_amount_change",
        "adjuster_id_policy_id_min_approved_amount",
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_type_claim_date_seq), F.lit(0)), 4).alias("approved_amount_share_of_claim_type_claim_date_s"),
        F.round(F.col("approved_amount_change") / F.nullif(F.sum("approved_amount_change").over(w_claim_type_claim_date_seq), F.lit(0)), 4).alias("approved_amount_change_share_of_claim_type_claim"),
        F.last("first_claim_date", ignorenulls=True).over(w_policy_id_by_approved_amount).alias("last_known_first_claim_date"),
        F.round(F.col("claimed_amount_max_3_adj") / F.nullif(F.sum("claimed_amount_max_3_adj").over(w_claim_type_claim_date_seq), F.lit(0)), 4).alias("claimed_amount_max_3_adj_share_of_claim_type_cla"),
        F.rank().over(w_policy_id_by_approved_amount_2).alias("approved_amount_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "policy_id",
        "claim_date",
        "approved_amount",
        "reported_at",
        "approved_amount_avg_1_prior",
        "first_claim_date",
        "claim_date_seq",
        "approved_amount_change",
        "approved_amount_share_of_claim_type_claim_date_s",
        "last_known_first_claim_date",
        "claimed_amount_max_3_adj_share_of_claim_type_cla",
        "approved_amount_rank",
    )
    .orderBy(F.col("id").desc(), F.col("claim_date_seq").desc_nulls_first())
)
