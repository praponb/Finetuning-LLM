from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_claim_claim_date_ties = Window.orderBy("claim_claim_date")
w_all_by_claim_claim_date = Window.orderBy("claim_claim_date", "id", "claim_id")
w_claim_type_claim_policy_id_b = Window.partitionBy("claim_type", "claim_policy_id").orderBy("claim_claim_date", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_claim_type_claim_policy_id_b_2 = Window.partitionBy("claim_type", "claim_policy_id").orderBy("claim_claim_date", "id", "claim_id")
w_all_by_claim_claim_date_full = Window.orderBy("claim_claim_date", "id", "claim_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_claims = df_claims.filter((F.col("status") == "APPROVED") & (F.col("adjuster_id") <= 4))

with_children = (
    cleaned_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.status",
        F.col("c.id").alias("claim_id"),
        F.col("c.claim_date").alias("claim_claim_date"),
        F.col("c.policy_id").alias("claim_policy_id"),
    )
)

with_trends = (
    with_children
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status",
        "claim_id",
        "claim_claim_date",
        "claim_policy_id",
        F.rank().over(w_all_by_claim_claim_date_ties).alias("claim_claim_date_rank"),
        F.row_number().over(w_all_by_claim_claim_date).alias("claim_claim_date_seq"),
        F.nth_value("claim_claim_date", 2).over(w_claim_type_claim_policy_id_b).alias("second_claim_claim_date"),
        F.rank().over(w_claim_type_claim_policy_id_b_2).alias("claim_claim_date_rank_2"),
        F.first("claim_claim_date").over(w_all_by_claim_claim_date_full).alias("first_claim_claim_date"),
    )
)

grouped = (
    with_trends
    .groupBy(
        "claim_claim_date_rank_2",
        F.date_trunc("month", "first_claim_claim_date").alias("first_claim_claim_date_month"),
        F.date_format("claim_claim_date", "yyyy-MM").alias("claim_claim_date_ym"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("status").isin("REJECTED", "IN_REVIEW", "PAID"), 1)).alias("n_status_in_rejected_in_review"),
        F.round(F.sum("claim_claim_date_rank_2") / F.nullif(F.sum("claim_claim_date_seq"), F.lit(0)), 3).alias("claim_claim_date_rank_2_to_claim_claim_date_seq_"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    grouped
    .filter(F.col("claim_claim_date_rank_2") < 9)
    .select(
        "claim_claim_date_rank_2",
        "claim_claim_date_ym",
        "cnt",
        "n_status_in_rejected_in_review",
        "claim_claim_date_rank_2_to_claim_claim_date_seq_",
        F.lower(F.regexp_replace(F.trim(F.col("claim_claim_date_ym")), " ", "_")).alias("claim_claim_date_ym_slug"),
    )
)
