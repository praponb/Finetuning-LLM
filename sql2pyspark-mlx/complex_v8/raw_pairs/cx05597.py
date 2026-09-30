from pyspark.sql import functions as F

valid_policys = (
    df_policies
    .filter(F.col("start_date").between(F.lit("2024-11-20").cast("date"), F.lit("2025-08-03").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("end_date")).alias("days_since_end_date"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

rollup_base = (
    valid_policys
    .filter(F.col("days_since_end_date") >= 70)
    .groupBy("holder_id", "status")
    .agg(
        F.round(F.sum("days_start_date_to_end_date") / F.nullif(F.sum("premium"), F.lit(0)), 3).alias("days_start_date_to_end_date_to_premium_ratio"),
        F.min("end_date").alias("first_end_date"),
        F.array_sort(F.collect_set("status")).alias("status_list"),
        F.round(F.sum("days_since_end_date") / F.nullif(F.sum("days_start_date_to_end_date"), F.lit(0)), 3).alias("days_since_end_date_to_days_start_date_to_end_da"),
        F.count("*").alias("n_rows"),
    )
)

exploded_detail_policys = (
    rollup_base.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.start_date").between(F.lit("2024-08-27").cast("date"), F.lit("2025-09-24").cast("date"))), "inner")
    .select(
        "s.holder_id",
        "s.status",
        "s.n_rows",
        F.col("p.id").alias("policy_id"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.status").alias("policy_status"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.end_date").alias("policy_end_date"),
    )
)

scored = (
    exploded_detail_policys
    .select(
        "holder_id",
        "status",
        "n_rows",
        "policy_id",
        "policy_start_date",
        "policy_premium",
        "policy_end_date",
        F.trunc(F.col("policy_start_date"), "MM").alias("policy_start_date_month_start"),
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
        F.concat(F.col("policy_status"), F.lit("#"), F.col("policy_id").cast("string")).alias("policy_status_ref"),
    )
)

without_matches = (
    scored.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.policy_id")) & (F.col("c.claim_type").isin("delay", "collision")) & (F.col("c.claim_date") < F.lit("2024-12-22").cast("date")), "left_anti")
    .filter(F.col("status_ref").isNotNull())
)

result = (
    without_matches
    .groupBy("policy_id")
    .pivot("status", ["EXPIRED", "ACTIVE", "LAPSED", "CANCELLED"])
    .agg(F.avg("n_rows"))
    .orderBy("policy_id")
)
