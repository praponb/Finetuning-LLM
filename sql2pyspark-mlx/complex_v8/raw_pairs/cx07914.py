from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("claim_type").orderBy(F.col("approved_amount").desc(), F.col("id").desc())
w_policy_id_status = Window.partitionBy("policy_id", "status")
w_reported_at_year_by_approved = Window.partitionBy("reported_at_year").orderBy(F.col("approved_amount").desc(), "id")
w_status_or_unknown_policy_id = Window.partitionBy("status_or_unknown", "policy_id").orderBy("approved_amount", "id")

recent_claims = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        F.year(F.col("reported_at")).alias("reported_at_year"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
    )
)

latest_per_key = (
    recent_claims
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

metrics = (
    latest_per_key
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        F.round(F.col("reported_at_year") - F.avg("reported_at_year").over(w_policy_id_status), 2).alias("reported_at_year_vs_policy_id_status_avg"),
        F.ntile(10).over(w_reported_at_year_by_approved).alias("approved_amount_decile"),
        F.round(F.col("approved_amount") / F.nullif(F.sum("approved_amount").over(w_policy_id_status), F.lit(0)), 4).alias("approved_amount_share_of_policy_id_status"),
        F.size(F.collect_set("claim_type").over(w_policy_id_status)).alias("distinct_claim_type_in_policy_id_status"),
        F.round(F.col("reported_at_year") / F.nullif(F.sum("reported_at_year").over(w_policy_id_status), F.lit(0)), 4).alias("reported_at_year_share_of_policy_id_status"),
        F.ntile(4).over(w_status_or_unknown_policy_id).alias("approved_amount_quartile"),
    )
)

stats = (
    metrics
    .filter(F.col("reported_at_year_share_of_policy_id_status") > 0.1)
    .groupBy(
        "approved_amount_decile",
        "approved_amount_quartile",
        "distinct_claim_type_in_policy_id_status",
    )
    .agg(
        F.sum("approved_amount_decile").alias("total_approved_amount_decile"),
        F.sum(F.when(F.col("claim_type").isin("collision", "fire"), F.col("approved_amount")).otherwise(0)).alias("approved_amount_claim_type_in_collision_fire"),
        F.round(F.stddev("approved_amount"), 2).alias("stddev_approved_amount"),
    )
)

result = (
    stats
    .select(
        "approved_amount_decile",
        "approved_amount_quartile",
        "total_approved_amount_decile",
        "distinct_claim_type_in_policy_id_status",
    )
    .unpivot(
        ids=["approved_amount_decile", "approved_amount_quartile"],
        values=["total_approved_amount_decile", "distinct_claim_type_in_policy_id_status"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
