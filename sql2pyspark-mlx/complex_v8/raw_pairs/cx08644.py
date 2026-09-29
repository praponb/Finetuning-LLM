from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_by_reported_at_ru = Window.partitionBy("claim_type").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type_by_reported_at = Window.partitionBy("claim_type").orderBy(F.col("reported_at").desc(), "id")
w_status_claim_date_month = Window.partitionBy("status", F.date_trunc("month", "claim_date"))
w_all_by_claim_date = Window.orderBy(F.col("claim_date").desc(), "id")
w_claim_date_month_status_by_c = Window.partitionBy(F.date_trunc("month", "claim_date"), "status").orderBy("claim_date", "id")
w_all_by_claim_date_ties = Window.orderBy(F.col("claim_date").desc())
w_policy_id_status = Window.partitionBy("policy_id", "status")

base = (
    df_claims
    .filter(F.col("reported_at") >= F.lit("2025-01-25 14:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
    )
)

with_windows = (
    base
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "reported_at",
        F.count(F.lit(1)).over(w_claim_type_by_reported_at_ru).alias("running_count"),
        F.ntile(4).over(w_claim_type_by_reported_at).alias("reported_at_quartile"),
        F.size(F.collect_set("adjuster_id").over(w_status_claim_date_month)).alias("distinct_adjuster_id_in_status_claim_date_month"),
        F.rank().over(w_all_by_claim_date).alias("claim_date_rank"),
        F.lead("claim_date").over(w_claim_date_month_status_by_c).alias("next_claim_date"),
        F.rank().over(w_all_by_claim_date_ties).alias("claim_date_rank_2"),
        F.round(F.percent_rank().over(w_claim_type_by_reported_at), 3).alias("reported_at_percent_rank"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id_status), F.lit(0)), 4).alias("approved_amount_share_of_policy_id_status"),
    )
)

labeled = (
    with_windows
    .filter(F.col("distinct_adjuster_id_in_status_claim_date_month") < 9)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "running_count",
        "reported_at_quartile",
        "claim_date_rank",
        "next_claim_date",
        "claim_date_rank_2",
        "reported_at_percent_rank",
        "approved_amount_share_of_policy_id_status",
        F.date_trunc("quarter", F.col("next_claim_date")).alias("next_claim_date_quarter"),
        F.lower(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.date_trunc("year", F.col("claim_date")).alias("claim_date_year"),
    )
)

result = (
    labeled
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        "running_count",
        "claim_date_rank_2",
        "reported_at_percent_rank",
        "approved_amount_share_of_policy_id_status",
        "next_claim_date_quarter",
        "claim_date_year",
        (F.floor(F.col("claim_date_rank") / 2) * 2).alias("claim_date_rank_bucket"),
    )
)
