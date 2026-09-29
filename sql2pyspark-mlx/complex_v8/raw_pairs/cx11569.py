from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_distinct_clai = Window.partitionBy("adjuster_id").orderBy(F.col("distinct_claim_types").desc(), "adjuster_id", "status", "claim_type")
w_status_by_distinct_claim_typ = Window.partitionBy("status").orderBy(F.col("distinct_claim_types").desc(), "adjuster_id", "status", "claim_type").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_claims
    .filter(F.col("reported_at") >= F.lit("2025-01-31 14:00:00").cast("timestamp"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
        F.concat_ws("|", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
    )
)

rollup_base = (
    cleaned
    .filter(F.col("status_claim_type_label").isNotNull())
    .groupBy("adjuster_id", "status", "claim_type")
    .agg(
        F.countDistinct("claim_type").alias("distinct_claim_types"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.sum(F.when(F.col("status") != "APPROVED", F.col("approved_amount")).otherwise(0)).alias("approved_amount_status_approved"),
    )
)

ranked = (
    rollup_base
    .select(
        "*",
        F.lead("distinct_claim_types").over(w_adjuster_id_by_distinct_clai).alias("next_distinct_claim_types"),
        F.round(F.cume_dist().over(w_adjuster_id_by_distinct_clai), 3).alias("distinct_claim_types_cume_dist"),
        F.sum("distinct_claim_types").over(w_status_by_distinct_claim_typ).alias("running_distinct_claim_types"),
    )
)

labeled = (
    ranked
    .withColumn("claim_type_ref", F.concat(F.col("claim_type"), F.lit("#"), F.col("adjuster_id").cast("string")))
)

result = (
    labeled
    .groupBy("claim_type")
    .pivot("status", ["APPROVED", "IN_REVIEW", "REJECTED", "OPEN"])
    .agg(F.count("next_distinct_claim_types").alias("count_v"), F.sum("next_distinct_claim_types").alias("sum_v"))
)
