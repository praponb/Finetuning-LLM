from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_adjuster_id_by_claimed_amoun = Window.partitionBy("adjuster_id").orderBy(F.col("claimed_amount").asc_nulls_last(), "id")
w_adjuster_id_by_reported_at = Window.partitionBy("adjuster_id").orderBy(F.col("reported_at").desc(), "id")

filtered = (
    df_claims
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
        F.coalesce(F.col("claim_type"), F.lit("UNKNOWN")).alias("claim_type_or_unknown"),
    )
)

with_trends = (
    filtered
    .withColumn("prev3_reported_at", F.lag("reported_at", 3).over(w_adjuster_id_by_claimed_amoun))
    .withColumn("prev3_claimed_amount", F.lag("claimed_amount", 3, 0).over(w_adjuster_id_by_reported_at))
)

per_group = (
    with_trends
    .filter(F.col("claimed_amount") >= 17759.3)
    .groupBy("claim_type")
    .agg(
        F.percentile_approx("claimed_amount", 0.5).alias("median_claimed_amount"),
        F.array_sort(F.collect_set("claim_type")).alias("claim_type_list"),
        F.count(F.when(~F.col("status").isin("OPEN", "REJECTED"), 1)).alias("n_status_not_in_open_rejected"),
        F.sum(F.when(F.col("claim_type").isin("medical", "theft"), F.col("prev3_claimed_amount"))).alias("prev3_claimed_amount_claim_type_in_medical_theft"),
        F.count("*").alias("row_count"),
    )
)

annotated = (
    per_group
    .select(
        "*",
        F.abs(F.col("row_count") - 5).alias("row_count_distance"),
        F.length(F.trim(F.col("claim_type"))).alias("claim_type_len"),
    )
)

annotated_2 = (
    annotated
    .select(
        "claim_type",
        "median_claimed_amount",
        "prev3_claimed_amount_claim_type_in_medical_theft",
        "row_count",
        "row_count_distance",
        "claim_type_len",
        F.initcap(F.trim(F.col("claim_type"))).alias("claim_type_clean"),
        F.coalesce(F.col("median_claimed_amount"), F.lit(0.0)).alias("median_claimed_amount_filled"),
    )
)

result = (
    annotated_2
    .filter(F.col("median_claimed_amount") < 35030.0)
    .select(
        "claim_type",
        "median_claimed_amount",
        "row_count",
        "row_count_distance",
        "claim_type_clean",
        F.round(F.col("median_claimed_amount") * 0.9, 2).alias("median_claimed_amount_adj"),
    )
    .orderBy(F.col("claim_type").desc_nulls_first(), F.col("row_count").desc())
    .limit(5)
)
