from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_reported_at_running = Window.orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_month_by_approved = Window.partitionBy(F.month("claim_date")).orderBy("approved_amount", "id")
w_policy_id = Window.partitionBy("policy_id")
w_reported_at_quarter_by_repor = Window.partitionBy("reported_at_quarter").orderBy("reported_at_quarter", "id")
w_reported_at_quarter_claim_da = Window.partitionBy("reported_at_quarter", F.date_trunc("month", "claim_date")).orderBy("reported_at_plus_7d", "id")
w_reported_at_quarter_adjuster = Window.partitionBy("reported_at_quarter", "adjuster_id").orderBy("reported_at_quarter")

cleaned = (
    df_claims
    .filter(F.col("policy_id") >= 16)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        (F.col("reported_at") + F.expr("INTERVAL 7 DAYS")).alias("reported_at_plus_7d"),
        F.quarter(F.col("reported_at")).alias("reported_at_quarter"),
    )
)

windowed = (
    cleaned
    .select(
        "*",
        F.last("approved_amount", ignorenulls=True).over(w_all_by_reported_at_running).alias("last_known_approved_amount"),
        F.row_number().over(w_claim_date_month_by_approved).alias("rn"),
        F.sum("claimed_amount").over(w_policy_id).alias("policy_id_sum_claimed_amount"),
        F.ntile(3).over(w_reported_at_quarter_by_repor).alias("reported_at_quarter_tercile"),
        F.lag("reported_at").over(w_reported_at_quarter_claim_da).alias("prev_reported_at"),
        F.round(F.percent_rank().over(w_reported_at_quarter_by_repor), 3).alias("reported_at_quarter_percent_rank"),
        F.round(F.col("reported_at_quarter") / F.nullif(F.sum("reported_at_quarter").over(w_policy_id), F.lit(0)), 4).alias("reported_at_quarter_share_of_policy_id"),
        F.round(F.avg("approved_amount").over(w_policy_id), 2).alias("policy_id_avg_approved_amount"),
        F.round(F.cume_dist().over(w_reported_at_quarter_adjuster), 3).alias("reported_at_quarter_cume_dist"),
        F.lag("claimed_amount").over(w_reported_at_quarter_claim_da).alias("prev_claimed_amount"),
    )
)

result = (
    windowed
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at_plus_7d",
        "reported_at_quarter",
        "last_known_approved_amount",
        "rn",
        "reported_at_quarter_tercile",
        "reported_at_quarter_percent_rank",
        "policy_id_avg_approved_amount",
        "reported_at_quarter_cume_dist",
        "prev_claimed_amount",
        (F.col("approved_amount") - F.col("claimed_amount")).alias("approved_amount_minus_claimed_amount"),
    )
    .orderBy(F.col("claim_date").desc_nulls_first(), "approved_amount", "id")
    .limit(20)
)
