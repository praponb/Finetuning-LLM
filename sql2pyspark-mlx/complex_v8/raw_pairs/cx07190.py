from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_date_year_by_approved = Window.partitionBy(F.year("claim_date")).orderBy("approved_amount", "id")
w_all_by_claimed_amount_plus_a = Window.orderBy(F.col("claimed_amount_plus_approved_amount").desc(), "id").rowsBetween(-2, -1)
w_all_by_claimed_amount_plus_a_2 = Window.orderBy(F.col("claimed_amount_plus_approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claimed_amount_plus_a_3 = Window.orderBy("claimed_amount_plus_approved_amount", "id")
w_all_by_claimed_amount_plus_a_4 = Window.orderBy(F.col("claimed_amount_plus_approved_amount").desc(), "id")
w_status = Window.partitionBy("status")
w_all_by_claimed_amount_plus_a_5 = Window.orderBy("claimed_amount_plus_approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_month = Window.partitionBy(F.date_trunc("month", "claim_date"))
w_all_by_approved_amount_ties = Window.orderBy(F.col("approved_amount").desc())
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")
w_claim_date_year_by_approved_2 = Window.partitionBy(F.year("claim_date")).orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_policy_id_by_policy_end_date = Window.partitionBy("policy_id").orderBy(F.col("policy_end_date").asc_nulls_last(), "id")
w_policy_id_by_policy_end_date_2 = Window.partitionBy("policy_id").orderBy(F.col("policy_end_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("policy_id") >= 27))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        (F.col("claimed_amount") + F.col("approved_amount")).alias("claimed_amount_plus_approved_amount"),
    )
)

enriched = (
    cleaned.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.approved_amount",
        "s.status",
        "s.claimed_amount_plus_approved_amount",
        F.col("p.end_date").alias("policy_end_date"),
    )
)

ranked = (
    enriched
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "claimed_amount_plus_approved_amount",
        "policy_end_date",
        F.row_number().over(w_claim_date_year_by_approved).alias("seq_no"),
        F.sum("approved_amount").over(w_all_by_claimed_amount_plus_a).alias("approved_amount_sum_2_prior"),
        F.first("approved_amount", ignorenulls=True).over(w_all_by_claimed_amount_plus_a_2).alias("first_known_approved_amount"),
        F.round(F.percent_rank().over(w_all_by_claimed_amount_plus_a_3), 3).alias("claimed_amount_plus_approved_amount_percent_rank"),
        F.lag("policy_end_date", 2).over(w_all_by_claimed_amount_plus_a_3).alias("prev2_policy_end_date"),
        F.rank().over(w_all_by_claimed_amount_plus_a_4).alias("claimed_amount_plus_approved_amount_rank"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_status), F.lit(0)), 4).alias("approved_amount_share_of_status"),
        F.count(F.lit(1)).over(w_all_by_claimed_amount_plus_a_5).alias("running_count"),
        F.size(F.collect_set("status").over(w_claim_date_month)).alias("distinct_status_in_claim_date_month"),
        F.round(F.percent_rank().over(w_all_by_approved_amount_ties), 3).alias("approved_amount_percent_rank"),
        F.row_number().over(w_all_by_approved_amount).alias("seq_no_2"),
        F.last("claim_date", ignorenulls=True).over(w_claim_date_year_by_approved_2).alias("last_known_claim_date"),
        F.lead("approved_amount").over(w_policy_id_by_policy_end_date).alias("next_approved_amount"),
        F.sum("approved_amount").over(w_policy_id_by_policy_end_date_2).alias("running_approved_amount"),
        F.count(F.lit(1)).over(w_claim_date_month).alias("rows_in_claim_date_month"),
    )
)

derived = (
    ranked
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("last_known_claim_date")).alias("days_since_last_known_claim_date"),
    )
)

result = (
    derived
    .select(
        "id",
        "policy_id",
        "status",
        "claimed_amount_plus_approved_amount",
        "seq_no",
        "approved_amount_sum_2_prior",
        "first_known_approved_amount",
        "prev2_policy_end_date",
        "claimed_amount_plus_approved_amount_rank",
        "approved_amount_share_of_status",
        "running_count",
        "distinct_status_in_claim_date_month",
        "approved_amount_percent_rank",
        "seq_no_2",
        "last_known_claim_date",
        "running_approved_amount",
        "rows_in_claim_date_month",
        "days_since_last_known_claim_date",
    )
    .orderBy(
        F.col("claimed_amount_plus_approved_amount_rank").desc(),
        F.col("approved_amount_share_of_status").asc(),
    )
)
