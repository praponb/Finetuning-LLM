from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_policy_id_status_by_approved = Window.partitionBy("policy_id", "status").orderBy("approved_amount", "id")
w_claim_type = Window.partitionBy("claim_type")
w_policy_id_status_by_claimed = Window.partitionBy("policy_id", "status").orderBy("claimed_amount", "id")
w_all_by_claim_date = Window.orderBy(F.col("claim_date").desc(), "id")
w_claim_type_approved_amount_s = Window.partitionBy("claim_type", "approved_amount_seq").orderBy(F.col("claimed_amount_filled_share_of_claim_type").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_approved_amount_seq_distinct = Window.partitionBy("approved_amount_seq", "distinct_policy_id_in_claim_type")

recent_claims = (
    df_claims
    .filter(F.col("adjuster_id") <= 6)
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
        F.coalesce(F.col("claimed_amount"), F.lit(0.0)).alias("claimed_amount_filled"),
    )
)

metrics = (
    recent_claims
    .select(
        "id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        F.row_number().over(w_policy_id_status_by_approved).alias("approved_amount_seq"),
        F.count(F.lit(1)).over(w_claim_type).alias("rows_in_claim_type"),
        F.round(F.col("claimed_amount_filled") - F.avg("claimed_amount_filled").over(w_claim_type), 2).alias("claimed_amount_filled_vs_claim_type_avg"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_claim_type), F.lit(0)), 4).alias("approved_amount_share_of_claim_type"),
        F.size(F.collect_set("policy_id").over(w_claim_type)).alias("distinct_policy_id_in_claim_type"),
        F.max("claimed_amount").over(w_claim_type).alias("claim_type_max_claimed_amount"),
        F.round(F.col("claimed_amount_filled") / F.nullif(F.sum("claimed_amount_filled").over(w_claim_type), F.lit(0)), 4).alias("claimed_amount_filled_share_of_claim_type"),
        F.sum("claimed_amount_filled").over(w_claim_type).alias("claim_type_sum_claimed_amount_filled"),
        F.round(F.cume_dist().over(w_policy_id_status_by_claimed), 3).alias("claimed_amount_cume_dist"),
        F.dense_rank().over(w_policy_id_status_by_claimed).alias("claimed_amount_dense_rank"),
        F.size(F.collect_set("adjuster_id").over(w_claim_type)).alias("distinct_adjuster_id_in_claim_type"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_type), 2).alias("claimed_amount_vs_claim_type_avg"),
        (F.col("claimed_amount") - F.lag("claimed_amount").over(w_all_by_claim_date)).alias("claimed_amount_change"),
    )
)

annotated = (
    metrics
    .select(
        "id",
        "claim_type",
        "approved_amount",
        "approved_amount_seq",
        "rows_in_claim_type",
        "claimed_amount_filled_vs_claim_type_avg",
        "distinct_policy_id_in_claim_type",
        "claim_type_max_claimed_amount",
        "claimed_amount_filled_share_of_claim_type",
        "claimed_amount_cume_dist",
        "claimed_amount_vs_claim_type_avg",
        "claimed_amount_change",
        F.round(F.col("claimed_amount_change") * 0.9, 2).alias("claimed_amount_change_adj"),
        F.upper(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.abs(F.col("rows_in_claim_type") - 10).alias("rows_in_claim_type_distance"),
    )
)

with_trends = (
    annotated
    .select(
        "id",
        "claim_type",
        "rows_in_claim_type",
        "claimed_amount_filled_vs_claim_type_avg",
        "distinct_policy_id_in_claim_type",
        "claimed_amount_filled_share_of_claim_type",
        "claimed_amount_vs_claim_type_avg",
        "claimed_amount_change",
        "claimed_amount_change_adj",
        "claim_type_clean",
        "rows_in_claim_type_distance",
        F.first("rows_in_claim_type_distance", ignorenulls=True).over(w_claim_type_approved_amount_s).alias("first_known_rows_in_claim_type_distance"),
        F.sum("claimed_amount_cume_dist").over(w_approved_amount_seq_distinct).alias("approved_amount_seq_distinct_policy_id_in_claim_"),
        F.max("approved_amount").over(w_approved_amount_seq_distinct).alias("approved_amount_seq_distinct_policy_id_in_claim__2"),
    )
)

result = (
    with_trends
    .filter(F.col("claimed_amount_filled_vs_claim_type_avg") <= 10000.0)
    .select(
        "id",
        "claim_type",
        "rows_in_claim_type",
        "claimed_amount_filled_vs_claim_type_avg",
        "claimed_amount_filled_share_of_claim_type",
        "claimed_amount_vs_claim_type_avg",
        "claimed_amount_change",
        "claim_type_clean",
        "rows_in_claim_type_distance",
        "first_known_rows_in_claim_type_distance",
        "approved_amount_seq_distinct_policy_id_in_claim_",
        "approved_amount_seq_distinct_policy_id_in_claim__2",
        F.least(F.col("rows_in_claim_type"), F.col("claimed_amount_change")).alias("least_rows_in_claim_type_claimed_amount_change"),
    )
    .orderBy(F.col("claim_type").desc())
)
