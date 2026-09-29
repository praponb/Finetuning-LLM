from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_status = Window.partitionBy("adjuster_id", "status")
w_adjuster_id_by_approved_amou = Window.partitionBy("adjuster_id").orderBy(F.col("approved_amount_minus_claimed_amount").desc(), "id")
w_adjuster_id = Window.partitionBy("adjuster_id")

valid_claims = (
    df_claims
    .filter(F.col("claim_type") == "theft")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "approved_amount",
        "status",
        "reported_at",
        (F.col("approved_amount") - F.col("claimed_amount")).alias("approved_amount_minus_claimed_amount"),
    )
)

labeled = (
    valid_claims
    .select(
        "*",
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
    )
)

metrics = (
    labeled
    .withColumn("distinct_policy_id_in_adjuster_id_status", F.size(F.collect_set("policy_id").over(w_adjuster_id_status)))
    .withColumn("approved_amount_minus_claimed_amount_seq", F.row_number().over(w_adjuster_id_by_approved_amou))
)

with_windows = (
    metrics
    .select(
        "*",
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_id), 2).alias("approved_amount_vs_adjuster_id_avg"),
        F.min("approved_amount_filled").over(w_adjuster_id).alias("adjuster_id_min_approved_amount_filled"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["distinct_policy_id_in_adjuster_id_status", "adjuster_id", "status"], ["distinct_policy_id_in_adjuster_id_status"]],
        "distinct_policy_id_in_adjuster_id_status", "adjuster_id", "status",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.count(F.when(F.col("status").isNotNull(), 1)).alias("n_status_is_not_null"),
        F.count("*").alias("row_count"),
    )
    .select(
        "distinct_policy_id_in_adjuster_id_status",
        "adjuster_id",
        F.coalesce(F.col("status"), F.lit("ALL")).alias("status"),
        "gid",
        "n_status_is_not_null",
        "row_count",
    )
    .orderBy(
        F.col("distinct_policy_id_in_adjuster_id_status").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
        F.col("status").asc_nulls_last(),
    )
)
