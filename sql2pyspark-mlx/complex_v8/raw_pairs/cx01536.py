from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_end_date_full = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_end_date = Window.orderBy("end_date", "id")
w_end_date_year_end_date_plus = Window.partitionBy(F.year("end_date"), F.date_trunc("month", "end_date_plus_90d"))
w_all_by_end_date_ties = Window.orderBy("end_date")

base = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2024-12-02").cast("date")) & (F.col("end_date").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_add(F.col("end_date"), 90).alias("end_date_plus_90d"),
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
        F.when(F.col("premium") >= 2945.9, "HIGH").when(F.col("premium") >= 2096.6, "MEDIUM").otherwise("LOW").alias("premium_tier"),
    )
)

labeled = (
    base
    .select(
        "id",
        "premium",
        "start_date",
        "end_date",
        "status",
        "end_date_plus_90d",
        "days_end_date_to_start_date",
        F.datediff(F.col("start_date"), F.col("end_date_plus_90d")).alias("days_end_date_plus_90d_to_start_date"),
    )
)

windowed = (
    labeled
    .select(
        "id",
        "premium",
        "start_date",
        "end_date",
        "end_date_plus_90d",
        "days_end_date_plus_90d_to_start_date",
        F.last("end_date_plus_90d").over(w_all_by_end_date_full).alias("last_end_date_plus_90d"),
        F.lead("start_date").over(w_all_by_end_date).alias("next_start_date"),
        F.round(F.avg("days_end_date_to_start_date").over(w_end_date_year_end_date_plus), 2).alias("end_date_year_end_date_plus_90d_month_avg_days_e"),
        F.ntile(4).over(w_all_by_end_date).alias("end_date_quartile"),
        F.round(F.cume_dist().over(w_all_by_end_date_ties), 3).alias("end_date_cume_dist"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.min("claim_date").alias("first_claim_date"),
        F.min("claimed_amount").alias("min_claim_claimed_amount"),
    )
)

profiled = (
    windowed.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.first_claim_date",
        "c.min_claim_claimed_amount",
    )
)

result = (
    profiled
    .filter(F.col("end_date_year_end_date_plus_90d_month_avg_days_e") <= 280.0)
    .select(
        "id",
        "premium",
        "start_date",
        "end_date",
        "end_date_plus_90d",
        "next_start_date",
        "end_date_year_end_date_plus_90d_month_avg_days_e",
        "end_date_quartile",
        "end_date_cume_dist",
        "claim_count",
        "first_claim_date",
        "min_claim_claimed_amount",
    )
    .orderBy(F.col("end_date").asc_nulls_last())
)
