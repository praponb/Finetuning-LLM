from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_by_approved_amount_ru = Window.partitionBy("status").orderBy("approved_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_status_by_approved_amount = Window.partitionBy("status").orderBy("approved_amount", "id")
w_policy_id_by_approved_amount = Window.partitionBy("policy_id").orderBy(F.col("approved_amount").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_by_approved_amount_2 = Window.partitionBy("policy_id").orderBy(F.col("approved_amount").desc())

filtered = (
    df_claims
    .filter(F.col("claimed_amount") <= 44991.5)
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
        F.concat(F.col("claim_type"), F.lit("#"), F.col("id").cast("string")).alias("claim_type_ref"),
    )
)

sequenced = (
    filtered
    .select(
        "id",
        "policy_id",
        "claim_type",
        "approved_amount",
        "status",
        F.min("approved_amount").over(w_status_by_approved_amount_ru).alias("min_approved_amount_so_far"),
        F.ntile(5).over(w_status_by_approved_amount).alias("approved_amount_quintile"),
        F.last("claimed_amount", ignorenulls=True).over(w_status_by_approved_amount_ru).alias("last_known_claimed_amount"),
        F.nth_value("reported_at", 2).over(w_policy_id_by_approved_amount).alias("second_reported_at"),
        F.dense_rank().over(w_policy_id_by_approved_amount_2).alias("approved_amount_dense_rank"),
        F.lag("claimed_amount").over(w_status_by_approved_amount).alias("prev_claimed_amount"),
    )
)

top_per_group = sequenced.filter(F.col("approved_amount_dense_rank") == 1)

totals = (
    top_per_group
    .groupBy("policy_id", "approved_amount_dense_rank", "claim_type")
    .agg(
        F.max("prev_claimed_amount").alias("max_prev_claimed_amount"),
        F.round(F.avg("last_known_claimed_amount"), 2).alias("avg_last_known_claimed_amount"),
        F.avg("prev_claimed_amount").alias("avg_prev_claimed_amount_check"),
    )
    .filter(F.col("avg_prev_claimed_amount_check") > 11235.6)
    .drop("avg_prev_claimed_amount_check")
)

result = (
    totals
    .select(
        "policy_id",
        "approved_amount_dense_rank",
        "claim_type",
        "max_prev_claimed_amount",
        F.coalesce(F.col("max_prev_claimed_amount"), F.lit(0.0)).alias("max_prev_claimed_amount_filled"),
    )
    .orderBy(
        F.col("max_prev_claimed_amount_filled").desc(),
        "policy_id",
        "approved_amount_dense_rank",
        "claim_type",
    )
    .limit(10)
)
