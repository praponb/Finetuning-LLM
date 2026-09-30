from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_month_claim_type = Window.partitionBy(F.date_trunc("month", "claim_date"), "claim_type")
w_claim_type_by_approved_amoun = Window.partitionBy("claim_type").orderBy(F.col("approved_amount").desc())
w_reported_at_month_by_claimed = Window.partitionBy(F.month("reported_at")).orderBy("claimed_amount", "id")
w_adjuster_id_policy_start_dat = Window.partitionBy("adjuster_id", F.year("policy_start_date")).orderBy(F.col("reported_at").desc())
w_claim_type_by_approved_amoun_2 = Window.partitionBy("claim_type").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id_claim_date_month = Window.partitionBy("adjuster_id", F.month("claim_date")).orderBy(F.col("claimed_amount").desc_nulls_first())
w_adjuster_id_policy_start_dat_2 = Window.partitionBy("adjuster_id", F.year("policy_start_date")).orderBy(F.col("reported_at").desc(), "id")
w_adjuster_id_claim_date_month_2 = Window.partitionBy("adjuster_id", F.month("claim_date")).orderBy(F.col("claimed_amount").desc_nulls_first(), "id")
w_adjuster_id_policy_start_dat_3 = Window.partitionBy("adjuster_id", F.year("policy_start_date")).orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = df_claims.filter((F.col("status") == "PAID") | (F.col("claimed_amount") <= 29191.2))

combined_details = (
    scoped.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.premium").alias("policy_premium"),
    )
)

metrics = (
    combined_details
    .select(
        "*",
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_claim_date_month_claim_type), F.lit(0)), 4).alias("claimed_amount_share_of_claim_date_month_claim_t"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_date_month_claim_type), 2).alias("claimed_amount_vs_claim_date_month_claim_type_av"),
        F.round(F.percent_rank().over(w_claim_type_by_approved_amoun), 3).alias("approved_amount_percent_rank"),
        F.round(F.percent_rank().over(w_reported_at_month_by_claimed), 3).alias("claimed_amount_percent_rank"),
        F.dense_rank().over(w_adjuster_id_policy_start_dat).alias("reported_at_dense_rank"),
        F.last("claim_date").over(w_claim_type_by_approved_amoun_2).alias("last_claim_date"),
        F.round(F.percent_rank().over(w_adjuster_id_claim_date_month), 3).alias("claimed_amount_percent_rank_2"),
        F.count(F.lit(1)).over(w_claim_date_month_claim_type).alias("rows_in_claim_date_month_claim_type"),
        F.lead("claim_date").over(w_adjuster_id_policy_start_dat_2).alias("next_claim_date"),
        F.min("approved_amount").over(w_claim_date_month_claim_type).alias("claim_date_month_claim_type_min_approved_amount"),
        F.row_number().over(w_adjuster_id_claim_date_month_2).alias("seq_no"),
        F.round(F.col("policy_premium") - F.avg("policy_premium").over(w_claim_date_month_claim_type), 2).alias("policy_premium_vs_claim_date_month_claim_type_av"),
        F.last("approved_amount").over(w_adjuster_id_policy_start_dat_3).alias("last_approved_amount"),
        F.first("claimed_amount").over(w_claim_type_by_approved_amoun_2).alias("first_claimed_amount"),
    )
)

first_rows = (
    metrics
    .filter(F.col("reported_at_dense_rank") <= 5)
    .drop("reported_at_dense_rank")
)

result = (
    first_rows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        "policy_start_date",
        "policy_premium",
        "claimed_amount_share_of_claim_date_month_claim_t",
        "approved_amount_percent_rank",
        "claimed_amount_percent_rank",
        "last_claim_date",
        "claimed_amount_percent_rank_2",
        "rows_in_claim_date_month_claim_type",
        "seq_no",
        "policy_premium_vs_claim_date_month_claim_type_av",
        "first_claimed_amount",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("next_claim_date")).alias("days_since_next_claim_date"),
    )
    .orderBy(F.col("days_since_next_claim_date").desc(), F.col("seq_no").asc())
)
