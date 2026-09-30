from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("holder_id").orderBy("premium", "id")
w_claim_claim_type_count_end_d = Window.partitionBy("claim_claim_type_count", F.date_trunc("month", "end_date")).orderBy(F.col("premium").desc(), "id").rowsBetween(-6, Window.currentRow)
w_claim_claim_type_count_by_cl = Window.partitionBy("claim_claim_type_count").orderBy("claim_claim_type_count", "id")
w_claim_claim_type_count_end_d_2 = Window.partitionBy("claim_claim_type_count", F.date_trunc("month", "end_date")).orderBy(F.col("premium").desc(), "id")
w_claim_claim_type_count_end_d_3 = Window.partitionBy("claim_claim_type_count", F.month("end_date"))

filtered_policys = (
    df_policies
    .filter(F.col("premium") >= 2033.8)
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

unique_rows = (
    filtered_policys
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.round(F.avg("approved_amount"), 2).alias("avg_claim_approved_amount"),
        F.min("reported_at").alias("first_claim_reported_at"),
        F.count(F.when(F.col("claim_type") == "flood", 1)).alias("claim_claim_type_count"),
    )
)

with_stats = (
    unique_rows.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.avg_claim_approved_amount",
        "c.first_claim_reported_at",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

metrics = (
    with_stats
    .select(
        "id",
        "holder_id",
        "status",
        "avg_claim_approved_amount",
        "claim_claim_type_count",
        F.round(F.avg("premium").over(w_claim_claim_type_count_end_d), 2).alias("premium_avg_7"),
        F.round(F.cume_dist().over(w_claim_claim_type_count_by_cl), 3).alias("claim_claim_type_count_cume_dist"),
        F.lead("avg_claim_approved_amount", 2).over(w_claim_claim_type_count_end_d_2).alias("next2_avg_claim_approved_amount"),
        F.sum("premium").over(w_claim_claim_type_count_end_d_3).alias("claim_claim_type_count_end_date_month_sum_premiu"),
        F.size(F.collect_set("holder_id").over(w_claim_claim_type_count_end_d_3)).alias("distinct_holder_id_in_claim_claim_type_count_end"),
    )
)

enriched = (
    metrics.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.risk_score") > 0.2), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.status",
        "s.claim_claim_type_count",
        "s.premium_avg_7",
        "s.claim_claim_type_count_cume_dist",
        F.col("p.risk_score").alias("holder_risk_score"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

qualified = (
    enriched.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claimed_amount") > 5774.6), "left_anti")
)

with_parent_holder = (
    qualified.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.segment") == "RETAIL"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.status",
        "s.claim_claim_type_count",
        "s.premium_avg_7",
        "s.claim_claim_type_count_cume_dist",
        "s.holder_risk_score",
        F.col("p.joined_on").alias("holder_joined_on_2"),
        F.col("p.segment").alias("holder_segment"),
        F.col("p.risk_score").alias("holder_risk_score_2"),
    )
)

result = (
    with_parent_holder
    .select(
        "id",
        "holder_id",
        "status",
        "claim_claim_type_count",
        "premium_avg_7",
        "claim_claim_type_count_cume_dist",
        "holder_risk_score",
        "holder_joined_on_2",
        "holder_segment",
        "holder_risk_score_2",
        F.coalesce(F.col("holder_segment"), F.lit("UNKNOWN")).alias("holder_segment_or_unknown"),
    )
    .orderBy(
        F.col("claim_claim_type_count_cume_dist").asc_nulls_last(),
        F.col("id").asc(),
    )
)
