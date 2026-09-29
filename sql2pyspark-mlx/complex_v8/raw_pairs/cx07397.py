from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_premium = Window.orderBy("premium", "id")
w_status_holder_id = Window.partitionBy("status", "holder_id")
w_all_by_premium_prior_1 = Window.orderBy("premium", "id").rowsBetween(-1, -1)
w_all_by_premium_running = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_premium_ties = Window.orderBy("premium")

cleaned_policys = (
    df_policies
    .filter((F.col("start_date") < F.lit("2025-05-30").cast("date")) | (F.col("holder_id") <= 6))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
        F.round(F.col("premium") * 1.5, 2).alias("premium_adj"),
        F.upper(F.trim(F.col("status"))).alias("status_clean"),
    )
)

with_details_holder = (
    cleaned_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.premium_adj",
        "s.status_clean",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.region").alias("holder_region"),
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

windowed = (
    with_details_holder
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_premium), 3).alias("premium_percent_rank"),
        F.sum("holder_risk_score").over(w_status_holder_id).alias("status_holder_id_sum_holder_risk_score"),
        F.round(F.avg("premium").over(w_all_by_premium_prior_1), 2).alias("premium_avg_1_prior"),
        F.last("start_date", ignorenulls=True).over(w_all_by_premium_running).alias("last_known_start_date"),
        F.round(F.percent_rank().over(w_all_by_premium_ties), 3).alias("premium_percent_rank_2"),
        F.first("premium_adj", ignorenulls=True).over(w_all_by_premium_running).alias("first_known_premium_adj"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.max("claimed_amount").alias("max_claim_claimed_amount"))
)

profiled = (
    windowed.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select("s.*", "c.max_claim_claimed_amount")
)

result = (
    profiled
    .filter(F.col("last_known_start_date").between(F.lit("2024-06-19").cast("date"), F.lit("2025-07-07").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        "premium_adj",
        "status_clean",
        "holder_segment",
        "holder_region",
        "holder_risk_score",
        "premium_percent_rank",
        "status_holder_id_sum_holder_risk_score",
        "last_known_start_date",
        "first_known_premium_adj",
        "max_claim_claimed_amount",
        F.add_months(F.col("start_date"), 6).alias("start_date_plus_6m"),
    )
    .orderBy(F.col("premium").asc_nulls_last(), "premium_adj")
)
