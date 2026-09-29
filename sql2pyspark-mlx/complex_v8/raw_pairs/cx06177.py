from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_by_reported_at_ru = Window.partitionBy("claim_type").orderBy(F.col("reported_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_reported_at_to_date = Window.partitionBy(F.to_date("reported_at"))
w_claim_type_by_reported_at = Window.partitionBy("claim_type").orderBy(F.col("reported_at").desc(), "id")
w_all_by_claim_date_full = Window.orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_claim_date = Window.orderBy(F.col("claim_date").desc(), "id")
w_all_by_claim_date_ties = Window.orderBy(F.col("claim_date").desc())
w_all_by_claimed_amount = Window.orderBy(F.col("claimed_amount").desc_nulls_first(), "id")
w_all_by_reported_at_percent_r = Window.orderBy("reported_at_percent_rank", "id")
w_reported_at_rank_by_claimed = Window.partitionBy("reported_at_rank").orderBy(F.col("claimed_amount").desc(), "id")

base = (
    df_claims
    .filter((F.col("reported_at") < F.lit("2025-03-08 14:00:00").cast("timestamp")) & (F.col("adjuster_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.when(F.col("approved_amount") >= 23120.7, "HIGH").when(F.col("approved_amount") >= 0.0, "MEDIUM").otherwise("LOW").alias("approved_amount_tier"),
    )
)

metrics = (
    base
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.first("claim_date", ignorenulls=True).over(w_claim_type_by_reported_at_ru).alias("first_known_claim_date"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_reported_at_to_date), 2).alias("claimed_amount_vs_reported_at_to_date_avg"),
        F.count(F.lit(1)).over(w_reported_at_to_date).alias("rows_in_reported_at_to_date"),
        F.rank().over(w_claim_type_by_reported_at).alias("reported_at_rank"),
        F.nth_value("approved_amount", 2).over(w_all_by_claim_date_full).alias("second_approved_amount"),
        F.lag("approved_amount", 2).over(w_all_by_claim_date).alias("prev2_approved_amount"),
        F.round(F.percent_rank().over(w_claim_type_by_reported_at), 3).alias("reported_at_percent_rank"),
        F.size(F.collect_set("status").over(w_reported_at_to_date)).alias("distinct_status_in_reported_at_to_date"),
        F.round(F.col("claimed_amount") / F.nullif(F.sum("claimed_amount").over(w_reported_at_to_date), F.lit(0)), 4).alias("claimed_amount_share_of_reported_at_to_date"),
        F.lag("reported_at", 3).over(w_claim_type_by_reported_at).alias("prev3_reported_at"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_reported_at_to_date), F.lit(0)), 4).alias("approved_amount_share_of_reported_at_to_date"),
        F.dense_rank().over(w_all_by_claim_date_ties).alias("claim_date_dense_rank"),
    )
)

windowed = (
    metrics
    .select(
        "*",
        F.round(F.cume_dist().over(w_all_by_claimed_amount), 3).alias("claimed_amount_cume_dist"),
        F.row_number().over(w_all_by_reported_at_percent_r).alias("reported_at_percent_rank_seq"),
        F.lag("prev3_reported_at").over(w_reported_at_rank_by_claimed).alias("prev_prev3_reported_at"),
    )
)

result = (
    windowed
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "first_known_claim_date",
        "rows_in_reported_at_to_date",
        "prev2_approved_amount",
        "reported_at_percent_rank",
        "claimed_amount_share_of_reported_at_to_date",
        "prev3_reported_at",
        "approved_amount_share_of_reported_at_to_date",
        "claim_date_dense_rank",
        "claimed_amount_cume_dist",
        "prev_prev3_reported_at",
        F.to_date(F.col("prev_prev3_reported_at")).alias("prev_prev3_reported_at_date"),
        F.abs(F.col("prev2_approved_amount") - 14993.0).alias("prev2_approved_amount_distance"),
    )
    .orderBy(F.col("reported_at_percent_rank").desc())
)
