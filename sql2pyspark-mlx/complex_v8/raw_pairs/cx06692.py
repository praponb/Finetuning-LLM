from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_status_by_reported = Window.partitionBy("policy_id", "status").orderBy("reported_at", "id")
w_status_adjuster_id = Window.partitionBy("status", "adjuster_id")
w_all_by_reported_at = Window.orderBy("reported_at", "id")
w_policy_id_status_by_reported_2 = Window.partitionBy("policy_id", "status").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_policy_start_date = Window.partitionBy("status").orderBy(F.col("policy_start_date").desc(), "id")
w_claim_date_year_adjuster_id = Window.partitionBy(F.year("claim_date"), "adjuster_id").orderBy(F.col("claimed_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_policy_start_date_2 = Window.partitionBy("status").orderBy(F.col("policy_start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_year_adjuster_id_2 = Window.partitionBy(F.year("claim_date"), "adjuster_id").orderBy(F.col("claimed_amount").desc(), "id")
w_all_by_first_known_policy_st = Window.orderBy(F.col("first_known_policy_start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_claim_date_month_by_s = Window.partitionBy("status", F.date_trunc("month", "claim_date")).orderBy("second_claim_date")
w_all_by_policy_start_date_per = Window.orderBy("policy_start_date_percent_rank", "id").rowsBetween(-6, -1)
w_first_known_policy_start_dat = Window.partitionBy(F.month("first_known_policy_start_date"))

cleaned = (
    df_claims
    .filter((F.to_date(F.col("reported_at")) < F.lit("2025-03-02").cast("date")) & (F.col("claim_type") == "delay"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        F.lower(F.regexp_replace(F.trim(F.col("claim_type")), " ", "_")).alias("claim_type_slug"),
    )
)

joined_policy = (
    cleaned.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.end_date") >= F.lit("2026-01-23").cast("date")), "inner")
    .select("s.*", F.col("p.start_date").alias("policy_start_date"))
)

with_trends = (
    joined_policy
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        "claim_type_slug",
        "policy_start_date",
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_policy_id_status_by_reported)).alias("days_since_prev_claim_date"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status_adjuster_id), 2).alias("claimed_amount_vs_status_adjuster_id_avg"),
        F.lag("claim_date").over(w_all_by_reported_at).alias("prev_claim_date"),
        F.max("claimed_amount").over(w_policy_id_status_by_reported_2).alias("max_claimed_amount_so_far"),
        F.round(F.percent_rank().over(w_status_by_policy_start_date), 3).alias("policy_start_date_percent_rank"),
        F.nth_value("claim_date", 2).over(w_claim_date_year_adjuster_id).alias("second_claim_date"),
        F.first("policy_start_date", ignorenulls=True).over(w_status_by_policy_start_date_2).alias("first_known_policy_start_date"),
        F.max("claimed_amount").over(w_status_adjuster_id).alias("status_adjuster_id_max_claimed_amount"),
        F.lead("claimed_amount", 3).over(w_claim_date_year_adjuster_id_2).alias("next3_claimed_amount"),
    )
)

scored = (
    with_trends
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("second_claim_date")).alias("days_since_second_claim_date"),
    )
)

ranked = (
    scored
    .select(
        "*",
        F.nth_value("days_since_second_claim_date", 2).over(w_all_by_first_known_policy_st).alias("second_days_since_second_claim_date"),
        F.rank().over(w_status_claim_date_month_by_s).alias("second_claim_date_rank"),
        F.sum("max_claimed_amount_so_far").over(w_all_by_policy_start_date_per).alias("max_claimed_amount_so_far_sum_6_prior"),
        F.round(F.col("max_claimed_amount_so_far") / F.nullif(F.sum("max_claimed_amount_so_far").over(w_first_known_policy_start_dat), F.lit(0)), 4).alias("max_claimed_amount_so_far_share_of_first_known_p"),
    )
)

result = (
    ranked
    .select(
        "id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        "policy_start_date",
        "days_since_prev_claim_date",
        "second_claim_date",
        "status_adjuster_id_max_claimed_amount",
        "next3_claimed_amount",
        "second_days_since_second_claim_date",
        "max_claimed_amount_so_far_share_of_first_known_p",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
    .orderBy(F.col("claimed_amount").desc_nulls_first())
)
