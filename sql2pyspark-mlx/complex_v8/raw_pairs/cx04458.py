from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_year_claim_date_m = Window.partitionBy("claim_date_year", F.date_trunc("month", "claim_date")).orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_date_year_claim_date_m_2 = Window.partitionBy("claim_date_year", F.date_trunc("month", "claim_date")).orderBy("claimed_amount", "id")
w_claim_date_year_by_greatest = Window.partitionBy("claim_date_year").orderBy(F.col("greatest_claimed_amount_approved_amount").desc(), "id")

cleaned_claims = (
    df_claims
    .filter(F.col("claimed_amount").between(13635.1, 41760.8))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.year(F.col("claim_date")).alias("claim_date_year"),
        F.greatest(F.col("claimed_amount"), F.col("approved_amount")).alias("greatest_claimed_amount_approved_amount"),
    )
)

with_windows = (
    cleaned_claims
    .select(
        "id",
        "claim_date",
        "reported_at",
        "claim_date_year",
        F.nth_value("reported_at", 2).over(w_claim_date_year_claim_date_m).alias("second_reported_at"),
        F.row_number().over(w_claim_date_year_claim_date_m_2).alias("claimed_amount_seq"),
        F.row_number().over(w_claim_date_year_by_greatest).alias("greatest_claimed_amount_approved_amount_seq"),
    )
)

grouped = (
    with_windows
    .filter(F.col("claimed_amount_seq") < 9)
    .groupBy(
        F.date_trunc("month", "second_reported_at").alias("second_reported_at_month"),
        "claim_date_year",
        "greatest_claimed_amount_approved_amount_seq",
    )
    .agg(
        F.min("reported_at").alias("first_reported_at"),
        F.max("reported_at").alias("last_reported_at"),
    )
)

filtered = (
    grouped
    .crossJoin(grouped.agg(F.max("greatest_claimed_amount_approved_amount_seq").alias("overall_max_greatest_claimed_amount_approved_amo")))
    .filter(F.col("greatest_claimed_amount_approved_amount_seq") < F.col("overall_max_greatest_claimed_amount_approved_amo"))
    .drop("overall_max_greatest_claimed_amount_approved_amo")
)

result = (
    filtered
    .select(
        "second_reported_at_month",
        "claim_date_year",
        "greatest_claimed_amount_approved_amount_seq",
        "first_reported_at",
        "last_reported_at",
        F.coalesce(F.col("claim_date_year"), F.lit(0)).alias("claim_date_year_filled"),
    )
    .orderBy(
        F.col("first_reported_at").desc(),
        F.col("second_reported_at_month").desc(),
    )
)
