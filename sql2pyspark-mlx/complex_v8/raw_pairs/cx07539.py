from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_claimed_amount_ful = Window.partitionBy("status").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type_claim_date_year_b = Window.partitionBy("claim_type", F.year("claim_date")).orderBy("claimed_amount", "id")
w_claim_date_year_claim_id = Window.partitionBy(F.year("claim_date"), "claim_id")
w_all_by_second_claim_date_tie = Window.orderBy("second_claim_date")
w_claim_status_claim_type_by_c = Window.partitionBy("claim_status", "claim_type").orderBy(F.col("claim_date").desc(), "id", "claim_id").rowsBetween(-3, -1)
w_all_by_second_claim_date = Window.orderBy("second_claim_date", "id", "claim_id")
w_all_by_second_claim_date_ful = Window.orderBy("second_claim_date", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_claims = (
    df_claims
    .filter((F.col("reported_at") >= F.lit("2025-01-19 14:00:00").cast("timestamp")) & (F.col("claimed_amount").between(7204.1, 41760.8)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
    )
)

windowed = (
    valid_claims
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "status",
        F.nth_value("claim_date", 2).over(w_status_by_claimed_amount_ful).alias("second_claim_date"),
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_claim_type_claim_date_year_b)).alias("days_since_prev_claim_date"),
    )
)

line_level_claims = (
    windowed.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.status",
        "s.second_claim_date",
        "s.days_since_prev_claim_date",
        F.col("c.id").alias("claim_id"),
        F.col("c.status").alias("claim_status"),
        "c.claim_type",
    )
)

with_windows = (
    line_level_claims
    .select(
        "*",
        F.round(F.col("days_since_prev_claim_date") - F.avg("days_since_prev_claim_date").over(w_claim_date_year_claim_id), 2).alias("days_since_prev_claim_date_vs_claim_date_year_cl"),
        F.rank().over(w_all_by_second_claim_date_tie).alias("second_claim_date_rank"),
        F.sum("days_since_prev_claim_date").over(w_claim_status_claim_type_by_c).alias("days_since_prev_claim_date_sum_3_prior"),
        F.count(F.lit(1)).over(w_claim_date_year_claim_id).alias("rows_in_claim_date_year_claim_id"),
        F.lead("second_claim_date", 2).over(w_all_by_second_claim_date).alias("next2_second_claim_date"),
        F.nth_value("claim_date", 2).over(w_all_by_second_claim_date_ful).alias("second_claim_date_2"),
    )
)

scored = (
    with_windows
    .filter(F.col("claim_type").isin("collision", "delay"))
    .withColumn("days_since_claim_date", F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")))
)

top_per_group = (
    scored
    .filter((F.col("second_claim_date_rank") == 1) & (F.col("claim_date") < F.lit("2025-01-10").cast("date")))
)

with_details_claim = (
    top_per_group.alias("s")
    .join(df_claims.alias("c"), F.col("c.id") == F.col("s.claim_id"), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.claim_date",
        "s.status",
        "s.second_claim_date",
        "s.claim_id",
        "s.claim_status",
        "s.claim_type",
        "s.days_since_prev_claim_date_vs_claim_date_year_cl",
        "s.second_claim_date_rank",
        "s.days_since_prev_claim_date_sum_3_prior",
        "s.next2_second_claim_date",
        "s.second_claim_date_2",
        "s.days_since_claim_date",
        F.col("c.status").alias("claim_status_2"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.adjuster_id").alias("claim_adjuster_id"),
        F.col("p.status").alias("policy_status"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

result = (
    with_details_claim
    .filter(F.col("claim_type") == "flood")
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_id",
        "claim_type",
        "days_since_prev_claim_date_vs_claim_date_year_cl",
        "second_claim_date_rank",
        "second_claim_date_2",
        "days_since_claim_date",
        "claim_status_2",
        "claim_claim_date",
        "claim_adjuster_id",
        "policy_status",
        "policy_end_date",
    )
    .orderBy(F.col("second_claim_date_2").desc())
)
