from pyspark.sql import functions as F

valid = (
    df_claims
    .filter((F.col("adjuster_id").isNotNull()) & (F.col("status").isin("IN_REVIEW", "APPROVED", "OPEN")))
)

stats = (
    valid
    .filter(F.col("policy_id") >= 12)
    .groupBy("claim_type")
    .agg(
        F.round(F.stddev("claimed_amount"), 2).alias("stddev_claimed_amount"),
        F.count("*").alias("row_count"),
        F.countDistinct("status").alias("distinct_status"),
    )
)

qualified = (
    stats
    .crossJoin(stats.agg(F.min("row_count").alias("overall_min_row_count")))
    .filter(F.col("row_count") > F.col("overall_min_row_count"))
    .drop("overall_min_row_count")
)

labeled = (
    qualified
    .select(
        "claim_type",
        "stddev_claimed_amount",
        "row_count",
        "distinct_status",
        F.coalesce(F.col("distinct_status"), F.lit(0)).alias("distinct_status_filled"),
    )
)

result = (
    labeled
    .select("claim_type", "distinct_status", "distinct_status_filled")
    .unpivot(
        ids=["claim_type"],
        values=["distinct_status", "distinct_status_filled"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
