from pyspark.sql import functions as F

valid = (
    df_policies
    .filter((F.col("status").isin("EXPIRED", "LAPSED", "ACTIVE")) & (F.year(F.col("start_date")) == 2025))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
        F.datediff(F.col("start_date"), F.col("end_date")).alias("days_end_date_to_start_date"),
    )
)

holder_id_stats = (
    valid
    .groupBy("holder_id")
    .agg(F.avg("days_end_date_to_start_date").alias("avg_days_end_date_to_start_date"))
)

vs_group = (
    valid.alias("s")
    .join(holder_id_stats.alias("g"), F.col("g.holder_id") == F.col("s.holder_id"), "inner")
    .filter(F.col("s.days_end_date_to_start_date") > F.col("g.avg_days_end_date_to_start_date"))
    .select(
        "s.*",
        "g.avg_days_end_date_to_start_date",
        F.round(F.col("s.days_end_date_to_start_date") - F.col("g.avg_days_end_date_to_start_date"), 2).alias("days_end_date_to_start_date_minus_avg"),
    )
)

scored = (
    vs_group
    .select(
        "id",
        "holder_id",
        "premium",
        "status",
        "product_or_unknown",
        "days_end_date_to_start_date",
        "avg_days_end_date_to_start_date",
        "days_end_date_to_start_date_minus_avg",
        F.date_trunc("week", F.col("end_date")).alias("end_date_week"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.max("end_date").alias("last_policy_end_date"))
)

summarized_rows = (
    scored.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.last_policy_end_date")
)

derived = (
    summarized_rows
    .filter(F.col("avg_days_end_date_to_start_date") <= 140.0)
    .withColumn("product_or_unknown_len", F.length(F.trim(F.col("product_or_unknown"))))
)

tagged = (
    derived
    .withColumn("product_or_unknown_len_per_premium", F.round(F.col("product_or_unknown_len") / F.nullif(F.col("premium"), F.lit(0)), 3))
)

joined = (
    tagged.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.status",
        "s.product_or_unknown",
        "s.days_end_date_to_start_date",
        "s.avg_days_end_date_to_start_date",
        "s.days_end_date_to_start_date_minus_avg",
        "s.end_date_week",
        "s.last_policy_end_date",
        "s.product_or_unknown_len_per_premium",
        F.col("p.risk_score").alias("holder_risk_score"),
    )
)

result = (
    joined
    .select(
        "id",
        "holder_id",
        "premium",
        "product_or_unknown",
        "days_end_date_to_start_date",
        "avg_days_end_date_to_start_date",
        "days_end_date_to_start_date_minus_avg",
        "end_date_week",
        "product_or_unknown_len_per_premium",
        "holder_risk_score",
    )
    .orderBy(F.col("product_or_unknown").desc())
)
