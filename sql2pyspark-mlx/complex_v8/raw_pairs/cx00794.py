from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("policy_id").orderBy(F.col("approved_amount_distance").desc(), F.col("id").desc())

cleaned_claims = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(9, 11)) | (F.col("claim_type") == "theft"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        F.concat(F.col("claim_type"), F.lit("#"), F.col("policy_id").cast("string")).alias("claim_type_ref"),
        F.abs(F.col("approved_amount") - 4868.7).alias("approved_amount_distance"),
        F.abs(F.col("claimed_amount") - 29336.0).alias("claimed_amount_distance"),
    )
)

latest = (
    cleaned_claims
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

tagged = (
    latest
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "reported_at",
        "approved_amount_distance",
        F.date_format(F.col("reported_at"), "yyyy-MM").alias("reported_at_label"),
    )
)

policy_id_stats = (
    tagged
    .groupBy("policy_id")
    .agg(F.percentile_approx("approved_amount_distance", 0.5).alias("median_approved_amount_distance"))
)

vs_group = (
    tagged.alias("s")
    .join(policy_id_stats.alias("g"), F.col("g.policy_id") == F.col("s.policy_id"), "inner")
    .filter(F.col("s.approved_amount_distance") >= F.col("g.median_approved_amount_distance"))
    .select(
        "s.*",
        "g.median_approved_amount_distance",
        F.round(F.col("s.approved_amount_distance") - F.col("g.median_approved_amount_distance"), 2).alias("approved_amount_distance_minus_median"),
    )
)

result = (
    vs_group
    .filter(F.col("median_approved_amount_distance") >= 17500.0)
    .select(
        "id",
        "policy_id",
        "claim_date",
        "claim_type",
        "reported_at",
        "approved_amount_distance",
        "reported_at_label",
        "median_approved_amount_distance",
        F.date_format(F.col("reported_at"), "MMM yyyy").alias("reported_at_label_2"),
        F.concat(F.col("claim_type"), F.lit("#"), F.col("policy_id").cast("string")).alias("claim_type_ref"),
    )
    .orderBy(F.col("reported_at_label_2").desc(), F.col("reported_at").desc(), "id")
    .limit(20)
)
