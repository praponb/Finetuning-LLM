from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_reported_at_date_year = Window.partitionBy("reported_at_date_year")
w_all_by_reported_at_date_year = Window.orderBy("reported_at_date_year", "id")
w_all_by_reported_at_date_full = Window.orderBy(F.col("reported_at_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_reported_at_date_year_policy = Window.partitionBy("reported_at_date_year", "policy_id").orderBy("next_reported_at", "id")
w_status_policy_id = Window.partitionBy("status", "policy_id")

valid = (
    df_claims
    .filter(F.col("policy_id") <= 3)
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
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        F.round(F.col("claimed_amount") / F.nullif(F.col("approved_amount"), F.lit(0)), 3).alias("claimed_amount_per_approved_amount"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.year(F.col("reported_at_date")).alias("reported_at_date_year"),
        F.when(F.col("claimed_amount") >= 33690.0, "large").when(F.col("claimed_amount") >= 19347.2, "medium").otherwise("small").alias("claimed_amount_tier"),
    )
)

scored_2 = (
    scored
    .filter(F.col("claimed_amount_per_approved_amount") >= 2.0)
    .select(
        "*",
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

windowed = (
    scored_2
    .select(
        "*",
        F.max("approved_amount_filled").over(w_reported_at_date_year).alias("reported_at_date_year_max_approved_amount_filled"),
        F.lead("reported_at").over(w_all_by_reported_at_date_year).alias("next_reported_at"),
        F.last("reported_at").over(w_all_by_reported_at_date_full).alias("last_reported_at"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.lead("claim_date", 3).over(w_reported_at_date_year_policy).alias("next3_claim_date"),
        F.max("claimed_amount_filled").over(w_status_policy_id).alias("status_policy_id_max_claimed_amount_filled"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_reported_at_date_year_policy)).alias("claimed_amount_change"),
    )
)

filtered = (
    with_windows
    .crossJoin(with_windows.agg(F.min("reported_at_date_year").alias("overall_min_reported_at_date_year")))
    .filter(F.col("reported_at_date_year") > F.col("overall_min_reported_at_date_year"))
    .drop("overall_min_reported_at_date_year")
)

result = (
    filtered
    .select(
        "id",
        F.col("claimed_amount_per_approved_amount").alias("m_claimed_amount_per_approved_amount"),
        F.col("approved_amount").alias("m_approved_amount"),
    )
    .unpivot(
        ids=["id"],
        values=["m_claimed_amount_per_approved_amount", "m_approved_amount"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
