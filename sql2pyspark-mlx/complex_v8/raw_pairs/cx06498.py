from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_approved_amount = Window.partitionBy("adjuster_id", "approved_amount_tier").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_adjuster_id_approved_amount_2 = Window.partitionBy("adjuster_id", "approved_amount_tier").orderBy("reported_at")
w_policy_id_status = Window.partitionBy("policy_id", "status")

cleaned = (
    df_claims
    .filter(F.col("approved_amount") >= 0.0)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "reported_at",
        F.when(F.col("approved_amount") >= 14668.0, "large").when(F.col("approved_amount") >= 4554.8, "medium").otherwise("small").alias("approved_amount_tier"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.max("claimed_amount").over(w_adjuster_id_approved_amount).alias("max_claimed_amount_so_far"),
        F.dense_rank().over(w_adjuster_id_approved_amount_2).alias("reported_at_dense_rank"),
        F.size(F.collect_set("adjuster_id").over(w_policy_id_status)).alias("distinct_adjuster_id_in_policy_id_status"),
    )
)

leaders = with_windows.filter(F.col("reported_at_dense_rank") <= 2)

result = (
    leaders
    .filter(F.col("reported_at_dense_rank") <= 8)
    .select(
        "id",
        "adjuster_id",
        "claimed_amount",
        "status",
        "reported_at",
        "approved_amount_tier",
        "max_claimed_amount_so_far",
        "reported_at_dense_rank",
    )
    .orderBy("approved_amount_tier")
)
