from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id = Window.partitionBy("adjuster_id")
w_policy_id_by_least_status_le = Window.partitionBy("policy_id").orderBy("least_status_len_claimed_amount", "id")
w_policy_id_by_least_status_le_2 = Window.partitionBy("policy_id").orderBy("least_status_len_claimed_amount")

valid = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2025-09-19").cast("date")) & (F.col("approved_amount") >= 0.0))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.concat(F.col("status"), F.lit("#"), F.col("policy_id").cast("string")).alias("status_ref"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.when(F.col("status").isin("IN_REVIEW", "APPROVED"), "group_a").when(F.col("status") == "OPEN", "group_b").otherwise("other").alias("status_group"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
        F.least(F.col("status_len"), F.col("claimed_amount")).alias("least_status_len_claimed_amount"),
    )
)

with_trends = (
    scored
    .select(
        "*",
        F.max("claimed_amount_bucket").over(w_adjuster_id).alias("adjuster_id_max_claimed_amount_bucket"),
        (F.col("status_len") - F.lag("status_len").over(w_policy_id_by_least_status_le)).alias("status_len_change"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_adjuster_id), 2).alias("approved_amount_vs_adjuster_id_avg"),
        F.count(F.lit(1)).over(w_adjuster_id).alias("rows_in_adjuster_id"),
        F.ntile(3).over(w_policy_id_by_least_status_le).alias("least_status_len_claimed_amount_tercile"),
        F.rank().over(w_policy_id_by_least_status_le_2).alias("least_status_len_claimed_amount_rank"),
    )
)

result = (
    with_trends
    .select("id", "approved_amount", "least_status_len_claimed_amount")
    .unpivot(
        ids=["id"],
        values=["approved_amount", "least_status_len_claimed_amount"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
