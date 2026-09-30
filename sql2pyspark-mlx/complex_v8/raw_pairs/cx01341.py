from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_claim_type_adjuster_id = Window.partitionBy("claim_type", "adjuster_id")
w_claim_date_month_by_claim_da = Window.partitionBy(F.month("claim_date")).orderBy("claim_date", "id")
w_claim_date_month_claim_type = Window.partitionBy(F.month("claim_date"), "claim_type").orderBy(F.col("claim_date").desc(), "id")

filtered = (
    df_claims
    .filter((F.month(F.col("reported_at")).isin(1, 2)) | (F.col("status").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        F.greatest(F.col("approved_amount"), F.col("claimed_amount")).alias("greatest_approved_amount_claimed_amount"),
    )
)

windowed = (
    filtered
    .select(
        "*",
        F.round(F.avg("approved_amount").over(w_claim_type_adjuster_id), 2).alias("claim_type_adjuster_id_avg_approved_amount"),
        F.round(F.col("approved_amount") - F.avg("approved_amount").over(w_claim_type_adjuster_id), 2).alias("approved_amount_vs_claim_type_adjuster_id_avg"),
        F.ntile(3).over(w_claim_date_month_by_claim_da).alias("claim_date_tercile"),
        F.rank().over(w_claim_date_month_claim_type).alias("claim_date_rank"),
    )
)

summary = (
    windowed
    .groupBy("claim_date_tercile")
    .agg(
        F.min("approved_amount").alias("min_approved_amount"),
        F.count("*").alias("n_rows"),
        F.sum("claim_type_adjuster_id_avg_approved_amount").alias("total_claim_type_adjuster_id_avg_approved_amount"),
        F.min("greatest_approved_amount_claimed_amount").alias("min_greatest_approved_amount_claimed_amount"),
    )
)

enriched = (
    summary
    .select(
        "*",
        (F.col("min_approved_amount") + F.col("claim_date_tercile")).alias("min_approved_amount_plus_claim_date_tercile"),
        F.greatest(F.col("claim_date_tercile"), F.col("min_approved_amount")).alias("greatest_claim_date_tercile_min_approved_amount"),
        (F.col("min_approved_amount") - F.col("claim_date_tercile")).alias("min_approved_amount_minus_claim_date_tercile"),
    )
)

tagged = (
    enriched
    .filter(F.col("min_approved_amount_minus_claim_date_tercile") >= 15003.0)
    .select(
        "claim_date_tercile",
        "min_approved_amount",
        "total_claim_type_adjuster_id_avg_approved_amount",
        "min_greatest_approved_amount_claimed_amount",
        "min_approved_amount_plus_claim_date_tercile",
        "min_approved_amount_minus_claim_date_tercile",
        F.abs(F.col("claim_date_tercile") - 6).alias("claim_date_tercile_distance"),
    )
)

labeled = (
    tagged
    .select(
        "*",
        F.coalesce(F.col("min_approved_amount"), F.lit(0.0)).alias("min_approved_amount_filled"),
    )
)

result = (
    labeled
    .select(
        "claim_date_tercile",
        "min_approved_amount",
        "total_claim_type_adjuster_id_avg_approved_amount",
        "claim_date_tercile_distance",
        "min_approved_amount_filled",
        (F.floor(F.col("min_approved_amount") / 10000) * 10000).alias("min_approved_amount_bucket"),
        (F.col("claim_date_tercile") - F.col("min_approved_amount")).alias("claim_date_tercile_minus_min_approved_amount"),
    )
    .orderBy(F.col("claim_date_tercile_distance").desc(), "claim_date_tercile")
    .limit(5)
)
