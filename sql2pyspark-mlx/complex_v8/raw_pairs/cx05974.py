from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("status").orderBy(F.col("reported_at").desc(), F.col("id").desc())
w_policy_id = Window.partitionBy("policy_id")
w_policy_id_status_len_by_clai = Window.partitionBy("policy_id", "status_len").orderBy("claimed_amount", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_policy_id_status_len_by_clai_2 = Window.partitionBy("policy_id", "status_len").orderBy("claimed_amount", "id")

cleaned = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("policy_id") >= 13))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
        F.length(F.trim(F.col("status"))).alias("status_len"),
    )
)

deduped = (
    cleaned
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

with_windows = (
    deduped
    .select(
        "*",
        F.round(F.col("status_len") - F.avg("status_len").over(w_policy_id), 2).alias("status_len_vs_policy_id_avg"),
        F.count(F.lit(1)).over(w_policy_id).alias("rows_in_policy_id"),
        F.nth_value("claim_date", 2).over(w_policy_id_status_len_by_clai).alias("second_claim_date"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_policy_id), 2).alias("claimed_amount_vs_policy_id_avg"),
        F.lead("claim_date").over(w_policy_id_status_len_by_clai_2).alias("next_claim_date"),
    )
)

with_details = (
    with_windows.alias("s")
    .join(df_adjusters.alias("a"), (F.col("a.id") == F.col("s.adjuster_id")) & (F.col("a.level").between(2, 3)), "inner")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.status_len",
        "s.status_len_vs_policy_id_avg",
        "s.rows_in_policy_id",
        "s.claimed_amount_vs_policy_id_avg",
        "s.next_claim_date",
        F.col("a.level").alias("adjuster_level"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

result = (
    with_details
    .groupingSets(
        [["policy_id", "adjuster_id", "adjuster_level"], ["policy_id"]],
        "policy_id", "adjuster_id", "adjuster_level",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.min("rows_in_policy_id").alias("min_rows_in_policy_id"),
        F.min("status_len_vs_policy_id_avg").alias("min_status_len_vs_policy_id_avg"),
    )
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
        F.col("adjuster_level").asc_nulls_last(),
    )
)
