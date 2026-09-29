from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("status").orderBy(F.col("premium").desc(), F.col("id").desc())

filtered = (
    df_policies
    .filter(F.col("premium") > 2013.8)
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

filtered_2 = (
    filtered
    .crossJoin(filtered.agg(F.avg("premium").alias("overall_avg_premium")))
    .filter(F.col("premium") > F.col("overall_avg_premium"))
    .drop("overall_avg_premium")
)

scored = (
    filtered_2
    .select(
        "id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_format(F.col("start_date"), "MMM yyyy").alias("start_date_label"),
    )
)

latest_per_key = (
    scored
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

filtered_3 = (
    latest_per_key.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left_semi")
    .filter(F.col("start_date_label").isNotNull())
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.min("reported_at").alias("first_claim_reported_at"),
        F.count(F.when(F.col("claim_type") != "medical", 1)).alias("claim_claim_type_count"),
    )
)

with_stats = (
    filtered_3.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "c.first_claim_reported_at",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

result = (
    with_stats
    .select(
        "id",
        "premium",
        "end_date",
        "status",
        "start_date_label",
        "first_claim_reported_at",
        "claim_claim_type_count",
    )
)
