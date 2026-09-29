from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("product", "status").orderBy("premium", "id")

valid = (
    df_policies
    .filter(F.col("premium") < 2945.9)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
    )
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

without_matches = (
    latest
    .crossJoin(latest.agg(F.min("premium").alias("overall_min_premium")))
    .filter(F.col("premium") > F.col("overall_min_premium"))
    .drop("overall_min_premium")
)

detail_rows = (
    without_matches.alias("s")
    .join(df_claims.alias("c"), F.col("c.policy_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.end_date",
        "s.status",
        F.col("c.id").alias("claim_id"),
        "c.claim_date",
        F.col("c.status").alias("claim_status"),
        "c.claim_type",
    )
)

result = (
    detail_rows
    .select(
        "id",
        "holder_id",
        "end_date",
        "claim_id",
        "claim_date",
        "claim_status",
        "claim_type",
        F.when(F.col("claim_status").isin("REJECTED", "IN_REVIEW"), "group_a").when(F.col("claim_status") == "PAID", "group_b").otherwise("other").alias("claim_status_group"),
    )
    .orderBy("claim_type", F.col("id").desc())
)
