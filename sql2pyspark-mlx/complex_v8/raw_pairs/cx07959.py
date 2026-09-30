from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_all_by_claim_date_running = Window.orderBy(F.col("claim_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_claim_date = Window.orderBy(F.col("claim_date").desc(), "id")
w_claim_type_len_seq_no = Window.partitionBy("claim_type_len", "seq_no")
w_policy_id_adjuster_id_by_cla = Window.partitionBy("policy_id", "adjuster_id").orderBy("claim_type_len_vs_status_avg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_claims
    .filter((F.col("approved_amount") >= 0.0) & (F.col("adjuster_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "reported_at",
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
    )
)

ranked = (
    recent
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status_prefix",
        "claim_type_len",
        F.round(F.col("claim_type_len") - F.avg("claim_type_len").over(w_status), 2).alias("claim_type_len_vs_status_avg"),
        F.sum("claim_type_len").over(w_all_by_claim_date_running).alias("running_claim_type_len"),
        F.row_number().over(w_all_by_claim_date).alias("seq_no"),
        F.round(F.col("claim_type_len") / F.nullif(F.sum("claim_type_len").over(w_status), F.lit(0)), 4).alias("claim_type_len_share_of_status"),
    )
)

ranked_2 = (
    ranked
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "status_prefix",
        "claim_type_len_vs_status_avg",
        "running_claim_type_len",
        "seq_no",
        "claim_type_len_share_of_status",
        F.round(F.col("running_claim_type_len") / F.nullif(F.sum("running_claim_type_len").over(w_claim_type_len_seq_no), F.lit(0)), 4).alias("running_claim_type_len_share_of_claim_type_len_s"),
        F.round(F.avg("running_claim_type_len").over(w_claim_type_len_seq_no), 2).alias("claim_type_len_seq_no_avg_running_claim_type_len"),
        F.round(F.avg("claim_type_len_vs_status_avg").over(w_claim_type_len_seq_no), 2).alias("claim_type_len_seq_no_avg_claim_type_len_vs_stat"),
        F.size(F.collect_set("policy_id").over(w_claim_type_len_seq_no)).alias("distinct_policy_id_in_claim_type_len_seq_no"),
        F.min("claim_type_len").over(w_policy_id_adjuster_id_by_cla).alias("min_claim_type_len_so_far"),
    )
)

scored = (
    ranked_2
    .filter(F.col("claim_type_len_seq_no_avg_claim_type_len_vs_stat") < 2.8)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status_prefix",
        "claim_type_len_vs_status_avg",
        "running_claim_type_len",
        "running_claim_type_len_share_of_claim_type_len_s",
        "distinct_policy_id_in_claim_type_len_seq_no",
        "min_claim_type_len_so_far",
        F.concat(F.col("status_prefix"), F.lit("#"), F.col("seq_no").cast("string")).alias("status_prefix_ref"),
    )
)

result = (
    scored
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "status_prefix",
        "claim_type_len_vs_status_avg",
        "running_claim_type_len_share_of_claim_type_len_s",
        "distinct_policy_id_in_claim_type_len_seq_no",
        "min_claim_type_len_so_far",
        "status_prefix_ref",
        (F.floor(F.col("running_claim_type_len") / 25) * 25).alias("running_claim_type_len_bucket"),
        F.when(F.col("running_claim_type_len") >= 112, "large").when(F.col("running_claim_type_len") >= 43, "medium").otherwise("small").alias("running_claim_type_len_tier"),
    )
    .orderBy(
        F.col("claim_type_len_vs_status_avg").desc(),
        "running_claim_type_len_tier",
    )
)
