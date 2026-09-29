from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_date_dayofweek = Window.orderBy("claim_date_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_adjuster_id = Window.partitionBy("adjuster_id")
w_all_by_claim_date_dayofweek_2 = Window.orderBy("claim_date_dayofweek", "id").rowsBetween(-2, Window.currentRow)
w_all_by_claim_date_dayofweek_3 = Window.orderBy("claim_date_dayofweek")
w_all_by_claim_date_dayofweek_4 = Window.orderBy("claim_date_dayofweek", "id")
w_all_by_approved_amount = Window.orderBy(F.col("approved_amount").desc(), "id")
w_policy_start_date_plus_6m_ye = Window.partitionBy(F.year("policy_start_date_plus_6m")).orderBy("claim_date_dayofweek_percent_rank", "id")
w_policy_start_date_plus_6m_ye_2 = Window.partitionBy(F.year("policy_start_date_plus_6m")).orderBy("claim_date_dayofweek_percent_rank")
w_all_by_approved_amount_ties = Window.orderBy(F.col("approved_amount").desc())

prepared = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
        F.dayofweek(F.col("claim_date")).alias("claim_date_dayofweek"),
    )
)

combined_details = (
    prepared.alias("s")
    .join(df_policies.alias("p"), (F.col("p.id") == F.col("s.policy_id")) & (F.col("p.start_date").between(F.lit("2024-03-15").cast("date"), F.lit("2025-05-28").cast("date"))), "left")
    .select(
        "s.*",
        F.col("p.status").alias("policy_status"),
        F.col("p.start_date").alias("policy_start_date"),
    )
)

metrics = (
    combined_details
    .select(
        "*",
        F.nth_value("reported_at", 2).over(w_all_by_claim_date_dayofweek).alias("second_reported_at"),
        F.round(F.col("claim_date_dayofweek") - F.avg("claim_date_dayofweek").over(w_adjuster_id), 2).alias("claim_date_dayofweek_vs_adjuster_id_avg"),
        F.sum("claim_date_dayofweek").over(w_adjuster_id).alias("adjuster_id_sum_claim_date_dayofweek"),
        F.nth_value("policy_start_date", 2).over(w_all_by_claim_date_dayofweek).alias("second_policy_start_date"),
        F.max("approved_amount").over(w_all_by_claim_date_dayofweek_2).alias("approved_amount_max_3"),
        F.round(F.cume_dist().over(w_all_by_claim_date_dayofweek_3), 3).alias("claim_date_dayofweek_cume_dist"),
        F.round(F.col("claim_date_dayofweek") / F.nullif(F.sum("claim_date_dayofweek").over(w_adjuster_id), F.lit(0)), 4).alias("claim_date_dayofweek_share_of_adjuster_id"),
        F.round(F.percent_rank().over(w_all_by_claim_date_dayofweek_4), 3).alias("claim_date_dayofweek_percent_rank"),
        F.max("claim_date_dayofweek").over(w_adjuster_id).alias("adjuster_id_max_claim_date_dayofweek"),
    )
)

annotated = (
    metrics
    .filter(F.month(F.col("second_reported_at")).isin(1, 3))
    .select(
        "*",
        F.date_format(F.col("reported_at"), "yyyy-MM").alias("reported_at_label"),
        F.add_months(F.col("policy_start_date"), 6).alias("policy_start_date_plus_6m"),
        F.date_trunc("day", F.col("reported_at")).alias("reported_at_day"),
    )
)

ranked = (
    annotated
    .select(
        "*",
        F.lead("approved_amount", 2).over(w_all_by_approved_amount).alias("next2_approved_amount"),
        F.ntile(3).over(w_policy_start_date_plus_6m_ye).alias("claim_date_dayofweek_percent_rank_tercile"),
        F.round(F.cume_dist().over(w_policy_start_date_plus_6m_ye_2), 3).alias("claim_date_dayofweek_percent_rank_cume_dist"),
        F.dense_rank().over(w_all_by_approved_amount_ties).alias("approved_amount_dense_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "policy_id",
        "approved_amount",
        "reported_at",
        "claim_date_dayofweek",
        "policy_status",
        "policy_start_date",
        "second_reported_at",
        "claim_date_dayofweek_vs_adjuster_id_avg",
        "adjuster_id_sum_claim_date_dayofweek",
        "second_policy_start_date",
        "approved_amount_max_3",
        "claim_date_dayofweek_cume_dist",
        "claim_date_dayofweek_share_of_adjuster_id",
        "adjuster_id_max_claim_date_dayofweek",
        "reported_at_label",
        "policy_start_date_plus_6m",
        "reported_at_day",
        "next2_approved_amount",
        "claim_date_dayofweek_percent_rank_tercile",
        "claim_date_dayofweek_percent_rank_cume_dist",
        "approved_amount_dense_rank",
        F.abs(F.col("claim_date_dayofweek") - 4).alias("claim_date_dayofweek_distance"),
    )
    .orderBy(F.col("next2_approved_amount").desc(), "id")
    .limit(5)
)
