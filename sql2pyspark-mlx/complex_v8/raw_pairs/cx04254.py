from pyspark.sql import functions as F

valid_policys = (
    df_policies
    .filter((~F.col("status").isin("ACTIVE", "CANCELLED")) | (F.col("end_date").between(F.lit("2024-11-08").cast("date"), F.lit("2027-01-03").cast("date"))))
    .select(
        "id",
        "holder_id",
        "start_date",
        "end_date",
        "status",
    )
)

derived = (
    valid_policys
    .filter(F.col("holder_id") <= 15)
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        F.date_trunc("quarter", F.col("end_date")).alias("end_date_quarter"),
        F.substring(F.trim(F.col("status")), 1, 3).alias("status_prefix"),
    )
)

scored = (
    derived
    .select(
        "*",
        F.date_trunc("quarter", F.col("end_date")).alias("end_date_quarter_2"),
        F.lower(F.trim(F.col("status_prefix"))).alias("status_prefix_clean"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(F.count("*").alias("claim_count"), F.count("*").alias("claim_count_2"))
)

summarized_rows = (
    scored.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_count_2"), F.lit(0)).alias("claim_count_2"),
    )
)

result = (
    summarized_rows
    .select("claim_count_2", "end_date_quarter", "status_prefix_clean")
    .distinct()
    .orderBy("status_prefix_clean")
)
