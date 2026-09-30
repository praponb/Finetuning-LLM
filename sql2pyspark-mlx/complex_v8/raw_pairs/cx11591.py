from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id = Window.partitionBy("policy_id")
w_status_policy_id_by_claim_da = Window.partitionBy("status", "policy_id").orderBy("claim_date", "id").rowsBetween(-2, Window.currentRow)
w_claim_type_by_claimed_amount = Window.partitionBy("claim_type").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type_by_claimed_amount_2 = Window.partitionBy("claim_type").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_month_by_claim_da = Window.partitionBy(F.date_trunc("month", "claim_date")).orderBy(F.col("claim_date").asc_nulls_last(), "id")
w_status_policy_id_by_claim_da_2 = Window.partitionBy("status", "policy_id").orderBy("claim_date")
w_claim_date_month_by_claim_da_2 = Window.partitionBy(F.date_trunc("month", "claim_date")).orderBy(F.col("claim_date").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_status_policy_id_by_claim_da_3 = Window.partitionBy("status", "policy_id").orderBy("claim_date", "id")
w_claim_type_by_claimed_amount_3 = Window.partitionBy("claim_type").orderBy("claimed_amount", "id")

cleaned = (
    df_claims
    .filter(F.col("status") == "PAID")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

enriched = (
    cleaned.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select("s.*", "a.adjuster_name", F.col("a.level").alias("adjuster_level"))
)

with_windows = (
    enriched
    .select(
        "*",
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.round(F.avg("adjuster_level").over(w_status_policy_id_by_claim_da), 2).alias("adjuster_level_avg_3"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_policy_id), F.lit(0)), 4).alias("claimed_amount_share_of_policy_id"),
        F.first("reported_at").over(w_claim_type_by_claimed_amount).alias("first_reported_at"),
        F.sum("claimed_amount").over(w_policy_id).alias("policy_id_sum_claimed_amount"),
        F.sum("adjuster_level").over(w_claim_type_by_claimed_amount_2).alias("running_adjuster_level"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_claim_date_month_by_claim_da)).alias("claimed_amount_change"),
        F.round(F.cume_dist().over(w_claim_date_month_by_claim_da), 3).alias("claim_date_cume_dist"),
        F.round(F.percent_rank().over(w_status_policy_id_by_claim_da_2), 3).alias("claim_date_percent_rank"),
        F.max("adjuster_level").over(w_claim_date_month_by_claim_da_2).alias("adjuster_level_max_6_prior"),
        F.lead("reported_at").over(w_status_policy_id_by_claim_da_3).alias("next_reported_at"),
        F.lag("claim_date").over(w_claim_type_by_claimed_amount_3).alias("prev_claim_date"),
        F.ntile(4).over(w_claim_type_by_claimed_amount_3).alias("claimed_amount_quartile"),
    )
)

annotated = (
    with_windows
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "adjuster_name",
        "adjuster_level",
        "rows_in_policy_id",
        "first_reported_at",
        "policy_id_sum_claimed_amount",
        "running_adjuster_level",
        "claimed_amount_change",
        "claim_date_cume_dist",
        "claim_date_percent_rank",
        "adjuster_level_max_6_prior",
        "next_reported_at",
        "claimed_amount_quartile",
        F.length(F.trim(F.col("adjuster_name"))).alias("adjuster_name_len"),
        F.datediff(F.col("claim_date"), F.col("prev_claim_date")).alias("days_prev_claim_date_to_claim_date"),
        F.round(F.col("running_adjuster_level") / F.nullif(F.col("claimed_amount"), F.lit(0)), 3).alias("running_adjuster_level_per_claimed_amount"),
    )
)

result = (
    annotated
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "adjuster_level",
        "policy_id_sum_claimed_amount",
        "running_adjuster_level",
        "claim_date_cume_dist",
        "adjuster_level_max_6_prior",
        "next_reported_at",
        "days_prev_claim_date_to_claim_date",
        "running_adjuster_level_per_claimed_amount",
        F.date_format(F.col("next_reported_at"), "yyyy-MM-dd").alias("next_reported_at_label"),
    )
    .orderBy("policy_id", "claim_type")
)
