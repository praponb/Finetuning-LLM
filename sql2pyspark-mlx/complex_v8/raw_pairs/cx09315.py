from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status = Window.partitionBy("status")
w_status_by_reported_at_runnin = Window.partitionBy("status").orderBy("reported_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_claim_type = Window.partitionBy("claim_type")
w_distinct_policy_id_in_status = Window.partitionBy("distinct_policy_id_in_status").orderBy(F.col("claimed_amount_vs_status_avg").desc())
w_distinct_policy_id_in_status_2 = Window.partitionBy("distinct_policy_id_in_status").orderBy(F.col("claimed_amount_vs_status_avg").desc(), "id").rowsBetween(-1, Window.currentRow)

prepared_claims = (
    df_claims
    .filter(F.col("status").isin("PAID", "APPROVED", "OPEN"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.date_format(F.col("reported_at"), "yyyy-MM-dd").alias("reported_at_label"),
        F.date_format(F.col("reported_at"), "MMM yyyy").alias("reported_at_label_2"),
    )
)

with_trends = (
    prepared_claims
    .select(
        "*",
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_status), 2).alias("claimed_amount_vs_status_avg"),
        F.size(F.collect_set("policy_id").over(w_status)).alias("distinct_policy_id_in_status"),
        F.sum("claimed_amount").over(w_status_by_reported_at_runnin).alias("running_claimed_amount"),
    )
)

sequenced = (
    with_trends
    .select(
        "*",
        F.count(F.lit(1)).over(w_claim_type).alias("rows_in_claim_type"),
        F.round(F.col("claimed_amount") - F.avg("claimed_amount").over(w_claim_type), 2).alias("claimed_amount_vs_claim_type_avg"),
        F.round(F.col("running_claimed_amount") - F.avg("running_claimed_amount").over(w_claim_type), 2).alias("running_claimed_amount_vs_claim_type_avg"),
    )
)

with_trends_2 = (
    sequenced
    .withColumn("claimed_amount_vs_status_avg_rank", F.rank().over(w_distinct_policy_id_in_status))
    .withColumn("running_claimed_amount_avg_2", F.round(F.avg("running_claimed_amount").over(w_distinct_policy_id_in_status_2), 2))
)

with_parent = (
    with_trends_2.alias("s")
    .join(df_policies.alias("p"), F.col("p.id") == F.col("s.policy_id"), "inner")
    .join(df_policyholders.alias("po"), F.col("po.id") == F.col("p.holder_id"), "left")
    .select(
        "s.id",
        "s.policy_id",
        "s.adjuster_id",
        "s.claim_type",
        "s.claimed_amount",
        "s.status",
        "s.reported_at",
        "s.reported_at_label",
        "s.reported_at_label_2",
        "s.claimed_amount_vs_status_avg",
        "s.distinct_policy_id_in_status",
        "s.rows_in_claim_type",
        "s.claimed_amount_vs_claim_type_avg",
        "s.running_claimed_amount_vs_claim_type_avg",
        "s.claimed_amount_vs_status_avg_rank",
        "s.running_claimed_amount_avg_2",
        F.col("p.end_date").alias("policy_end_date"),
        F.col("p.product").alias("policy_product"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("po.region").alias("holder_region"),
    )
)

result = (
    with_parent
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        "reported_at_label",
        "reported_at_label_2",
        "claimed_amount_vs_status_avg",
        "distinct_policy_id_in_status",
        "rows_in_claim_type",
        "running_claimed_amount_vs_claim_type_avg",
        "claimed_amount_vs_status_avg_rank",
        "running_claimed_amount_avg_2",
        "policy_product",
        F.when(F.col("rows_in_claim_type") >= 13, "HIGH").when(F.col("rows_in_claim_type") >= 5, "MEDIUM").otherwise("LOW").alias("rows_in_claim_type_tier"),
    )
    .orderBy(F.col("reported_at").asc(), "running_claimed_amount_vs_claim_type_avg")
)
