from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_by_claimed_amount = Window.partitionBy("claim_type").orderBy(F.col("claimed_amount").desc(), "id")
w_all_by_claim_date_running = Window.orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claimed_amount_runnin = Window.orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_adjuster_id = Window.partitionBy("claim_type", "adjuster_id")
w_claim_type_by_approved_amoun = Window.partitionBy("claim_type").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claim_date_moving_3 = Window.orderBy("claim_date", "id").rowsBetween(-3, Window.currentRow)
w_status_claim_date_year_by_cl = Window.partitionBy("status", F.year("claim_date")).orderBy("claimed_amount", "id")

scoped = (
    df_claims
    .filter((F.col("claim_type").isin("fire", "collision")) & (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.ntile(5).over(w_claim_type_by_claimed_amount).alias("claimed_amount_quintile"),
        F.max("claimed_amount").over(w_all_by_claim_date_running).alias("max_claimed_amount_so_far"),
        F.sum("days_since_claim_date").over(w_all_by_claimed_amount_runnin).alias("running_days_since_claim_date"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_type_adjuster_id), F.lit(0)), 4).alias("approved_amount_share_of_claim_type_adjuster_id"),
        F.first("days_since_claim_date", ignorenulls=True).over(w_claim_type_by_approved_amoun).alias("first_known_days_since_claim_date"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_type_adjuster_id), 2).alias("approved_amount_vs_claim_type_adjuster_id_avg"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_claim_type_by_claimed_amount)).alias("claimed_amount_change"),
        F.sum("approved_amount").over(w_all_by_claim_date_moving_3).alias("approved_amount_sum_4"),
        F.round(F.percent_rank().over(w_status_claim_date_year_by_cl), 3).alias("claimed_amount_percent_rank"),
    )
)

result = (
    with_windows
    .filter(F.col("approved_amount_vs_claim_type_adjuster_id_avg").between(-30000.0, 40000.0))
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "claimed_amount_quintile",
        "max_claimed_amount_so_far",
        "approved_amount_share_of_claim_type_adjuster_id",
        "first_known_days_since_claim_date",
        "approved_amount_vs_claim_type_adjuster_id_avg",
        "claimed_amount_change",
        "approved_amount_sum_4",
        F.quarter(F.col("claim_date")).alias("claim_date_quarter"),
    )
    .orderBy(
        F.col("approved_amount").desc(),
        F.col("claim_date_quarter").desc_nulls_first(),
    )
)
