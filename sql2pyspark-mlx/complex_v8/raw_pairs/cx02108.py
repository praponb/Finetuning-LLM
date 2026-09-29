from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_status_by_reported = Window.partitionBy("policy_id", "status").orderBy("reported_at", "id")
w_policy_id_status_by_reported_2 = Window.partitionBy("policy_id", "status").orderBy("reported_at")
w_reported_at_dense_rank_polic = Window.partitionBy("reported_at_dense_rank", "policy_id").orderBy("reported_at_dense_rank", "id")
w_next_reported_at_month_statu = Window.partitionBy(F.date_trunc("month", "next_reported_at"), "status").orderBy("next_claim_date", "id")
w_reported_at_dense_rank = Window.partitionBy("reported_at_dense_rank")
w_status_next_claim_date_month = Window.partitionBy("status", F.date_trunc("month", "next_claim_date")).orderBy("days_since_prev_claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_claims
    .filter(F.col("claim_type") == "medical")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "status",
        "reported_at",
        F.concat_ws(": ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
    )
)

joined_adjuster = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "inner")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_date",
        "s.status",
        "s.reported_at",
        "s.claim_type_status_label",
        "a.adjuster_name",
    )
)

ranked = (
    joined_adjuster
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        "reported_at",
        "claim_type_status_label",
        "adjuster_name",
        F.datediff(F.col("claim_date"), F.lag("claim_date").over(w_policy_id_status_by_reported)).alias("days_since_prev_claim_date"),
        F.lead("reported_at").over(w_policy_id_status_by_reported).alias("next_reported_at"),
        F.lead("claim_date").over(w_policy_id_status_by_reported).alias("next_claim_date"),
        F.dense_rank().over(w_policy_id_status_by_reported_2).alias("reported_at_dense_rank"),
    )
)

enriched = (
    ranked
    .filter(F.lower(F.col("adjuster_name")).like("%wa%"))
    .select(
        "*",
        F.upper(F.trim(F.col("claim_type_status_label"))).alias("claim_type_status_label_clean"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.ntile(10).over(w_reported_at_dense_rank_polic).alias("reported_at_dense_rank_decile"),
        F.rank().over(w_next_reported_at_month_statu).alias("next_claim_date_rank"),
        F.row_number().over(w_reported_at_dense_rank_polic).alias("rn"),
        F.round(F.col("reported_at_dense_rank") - F.avg("reported_at_dense_rank").over(w_reported_at_dense_rank), 2).alias("reported_at_dense_rank_vs_reported_at_dense_rank"),
        F.nth_value("reported_at_dense_rank", 2).over(w_status_next_claim_date_month).alias("second_reported_at_dense_rank"),
    )
)

adjuster_id_stats = (
    with_windows
    .groupBy("adjuster_id")
    .agg(F.max("reported_at_dense_rank_vs_reported_at_dense_rank").alias("max_reported_at_dense_rank_vs_reported_at_dense_"))
)

above_average = (
    with_windows.alias("s")
    .join(adjuster_id_stats.alias("g"), F.col("g.adjuster_id") == F.col("s.adjuster_id"), "inner")
    .filter(F.col("s.reported_at_dense_rank_vs_reported_at_dense_rank") == F.col("g.max_reported_at_dense_rank_vs_reported_at_dense_"))
    .select(
        "s.*",
        "g.max_reported_at_dense_rank_vs_reported_at_dense_",
        F.round(F.col("s.reported_at_dense_rank_vs_reported_at_dense_rank") - F.col("g.max_reported_at_dense_rank_vs_reported_at_dense_"), 2).alias("reported_at_dense_rank_vs_reported_at_dense_rank_2"),
    )
)

result = (
    above_average
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        "reported_at",
        "claim_type_status_label",
        "adjuster_name",
        "days_since_prev_claim_date",
        "next_reported_at",
        "next_claim_date",
        "reported_at_dense_rank",
        "claim_type_status_label_clean",
        "reported_at_dense_rank_decile",
        "next_claim_date_rank",
        "rn",
        "second_reported_at_dense_rank",
        "max_reported_at_dense_rank_vs_reported_at_dense_",
        "reported_at_dense_rank_vs_reported_at_dense_rank_2",
    )
    .orderBy(
        F.col("reported_at_dense_rank_decile").desc(),
        F.col("policy_id").desc_nulls_first(),
        "id",
    )
    .limit(10)
)
