from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_date_full = Window.orderBy("claim_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claim_date_moving_1 = Window.orderBy("claim_date", "id").rowsBetween(-1, Window.currentRow)
w_all_by_claim_date = Window.orderBy("claim_date", "id")
w_all_by_claim_date_ties = Window.orderBy(F.col("claim_date").desc())
w_all_by_first_approved_amount = Window.orderBy(F.col("first_approved_amount").desc(), "id")
w_status_adjuster_id_by_next_r = Window.partitionBy("status", "adjuster_id").orderBy("next_reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_adjuster_id_by_next_r_2 = Window.partitionBy("status", "adjuster_id").orderBy("next_reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_date_month_adjuster_le = Window.partitionBy(F.month("claim_date"), "adjuster_level")

cleaned = (
    df_claims
    .filter(F.col("claim_type") == "flood")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.date_format(F.col("reported_at"), "yyyy-MM").alias("reported_at_label"),
    )
)

ranked = (
    cleaned
    .select(
        "*",
        F.first("approved_amount").over(w_all_by_claim_date_full).alias("first_approved_amount"),
        F.round(F.avg("approved_amount").over(w_all_by_claim_date_moving_1), 2).alias("approved_amount_avg_2"),
        F.lead("reported_at").over(w_all_by_claim_date).alias("next_reported_at"),
        F.dense_rank().over(w_all_by_claim_date_ties).alias("claim_date_dense_rank"),
    )
)

filtered = (
    ranked.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.claim_date") >= F.lit("2024-07-18").cast("date")) & (F.col("c.reported_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp")), "left_anti")
)

joined_adjuster = (
    filtered.alias("s")
    .join(df_adjusters.alias("a"), F.col("a.id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.col("a.level").alias("adjuster_level"),
        F.col("a.team").alias("adjuster_team"),
        "a.adjuster_name",
    )
)

with_trends = (
    joined_adjuster
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status",
        "reported_at_label",
        "first_approved_amount",
        "approved_amount_avg_2",
        "next_reported_at",
        "claim_date_dense_rank",
        "adjuster_level",
        "adjuster_team",
        F.round((F.unix_timestamp("reported_at") - F.unix_timestamp(F.lag("reported_at").over(w_all_by_first_approved_amount))) / 60, 1).alias("minutes_since_prev_reported_at"),
        F.lead("first_approved_amount", 2).over(w_all_by_first_approved_amount).alias("next2_first_approved_amount"),
        F.first("next_reported_at").over(w_status_adjuster_id_by_next_r).alias("first_next_reported_at"),
        F.max("approved_amount").over(w_status_adjuster_id_by_next_r_2).alias("max_approved_amount_so_far"),
        F.size(F.collect_set("claim_type").over(w_claim_date_month_adjuster_le)).alias("distinct_claim_type_in_claim_date_month_adjuster"),
    )
)

result = (
    with_trends
    .groupingSets(
        [["claim_date_dense_rank", "status", "distinct_claim_type_in_claim_date_month_adjuster"], ["claim_date_dense_rank"], []],
        "claim_date_dense_rank", "status", "distinct_claim_type_in_claim_date_month_adjuster",
    )
    .agg(
        F.countDistinct("policy_id").alias("distinct_policy_ids"),
        F.count("*").alias("n_rows"),
    )
    .select(
        "claim_date_dense_rank",
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "distinct_claim_type_in_claim_date_month_adjuster",
        "distinct_policy_ids",
        "n_rows",
    )
)
