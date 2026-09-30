from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_status_holder_id_by_cl = Window.partitionBy("claim_status", "holder_id").orderBy("claim_reported_at", "id", "claim_id").rowsBetween(-2, -1)
w_claim_status_by_claim_report = Window.partitionBy("claim_status").orderBy("claim_reported_at", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_status_by_claim_report_2 = Window.partitionBy("claim_status").orderBy("claim_reported_at")
w_claim_status_by_claim_report_3 = Window.partitionBy("claim_status").orderBy("claim_reported_at", "id", "claim_id")
w_holder_id_claim_id = Window.partitionBy("holder_id", "claim_id")

cleaned_policys = (
    df_policies
    .filter(F.col("product") == "HOME")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.date_trunc("year", F.col("end_date")).alias("end_date_year"),
    )
)

with_children = (
    cleaned_policys.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.status_clean",
        "s.days_start_date_to_end_date",
        "s.end_date_year",
        F.col("c.id").alias("claim_id"),
        F.col("c.approved_amount").alias("claim_approved_amount"),
        F.col("c.status").alias("claim_status"),
        F.col("c.reported_at").alias("claim_reported_at"),
    )
)

screened = (
    with_children.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status") == "ACTIVE") & (F.col("p.product").isNotNull()), "left_anti")
)

metrics = (
    screened
    .select(
        "id",
        "holder_id",
        "premium",
        "status_clean",
        "end_date_year",
        "claim_id",
        "claim_status",
        F.round(F.avg("premium").over(w_claim_status_holder_id_by_cl), 2).alias("premium_avg_2_prior"),
        F.min("days_start_date_to_end_date").over(w_claim_status_by_claim_report).alias("min_days_start_date_to_end_date_so_far"),
        F.round(F.percent_rank().over(w_claim_status_by_claim_report_2), 3).alias("claim_reported_at_percent_rank"),
        F.round((F.unix_timestamp("end_date_year") - F.unix_timestamp(F.lag("end_date_year").over(w_claim_status_by_claim_report_3))) / 60, 1).alias("minutes_since_prev_end_date_year"),
        F.round(F.col("claim_approved_amount") - F.avg("claim_approved_amount").over(w_holder_id_claim_id), 2).alias("claim_approved_amount_vs_holder_id_claim_id_avg"),
    )
)

with_children_2 = (
    metrics.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.status").isin("IN_REVIEW", "OPEN")), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.end_date_year",
        "s.claim_id",
        "s.claim_status",
        "s.premium_avg_2_prior",
        "s.min_days_start_date_to_end_date_so_far",
        "s.claim_reported_at_percent_rank",
        "s.minutes_since_prev_end_date_year",
        "s.claim_approved_amount_vs_holder_id_claim_id_avg",
        F.col("c.id").alias("claim_id_2"),
        "c.claim_type",
        F.col("c.status").alias("claim_status_2"),
    )
)

filtered = (
    with_children_2.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left_semi")
)

qualified = (
    filtered.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_date") >= F.lit("2024-04-11").cast("date")) & (F.col("c.claimed_amount").between(3192.1, 32441.2)), "left_semi")
    .filter(F.col("claim_approved_amount_vs_holder_id_claim_id_avg") > -15000.0)
)

result = (
    qualified
    .select(
        "id",
        "claim_id",
        "claim_approved_amount_vs_holder_id_claim_id_avg",
        "minutes_since_prev_end_date_year",
        "claim_reported_at_percent_rank",
    )
    .unpivot(
        ids=["id", "claim_id"],
        values=["claim_approved_amount_vs_holder_id_claim_id_avg", "minutes_since_prev_end_date_year", "claim_reported_at_percent_rank"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
