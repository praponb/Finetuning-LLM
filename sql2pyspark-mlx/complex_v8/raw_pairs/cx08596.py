from pyspark.sql import functions as F

scoped_policys = (
    df_policies
    .filter((~F.col("status").isin("EXPIRED", "CANCELLED")) & (F.col("product") == "HEALTH"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("end_date")).alias("days_since_end_date"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.abs(F.col("premium") - 3521.8).alias("premium_distance"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.min("start_date").alias("first_policy_start_date"))
)

profiled = (
    scoped_policys.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select("s.*", "p.first_policy_start_date")
)

tagged = (
    profiled
    .filter(F.col("days_since_end_date") > 280)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        "days_since_end_date",
        "status_len",
        "premium_distance",
        F.greatest(F.col("days_since_end_date"), F.col("status_len")).alias("greatest_days_since_end_date_status_len"),
        F.when(F.col("premium_distance") >= 4000.0, "large").when(F.col("premium_distance") >= 1250.0, "medium").otherwise("small").alias("premium_distance_tier"),
        F.datediff(F.col("start_date"), F.col("first_policy_start_date")).alias("days_first_policy_start_date_to_start_date"),
    )
)

rollup_base = (
    tagged
    .groupBy("product")
    .agg(
        F.min("days_since_end_date").alias("min_days_since_end_date"),
        F.max("days_since_end_date").alias("max_days_since_end_date"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") >= 3)
)

with_activity = (
    rollup_base
    .crossJoin(rollup_base.agg(F.min("max_days_since_end_date").alias("overall_min_max_days_since_end_date")))
    .filter(F.col("max_days_since_end_date") > F.col("overall_min_max_days_since_end_date"))
    .drop("overall_min_max_days_since_end_date")
)

enriched = (
    with_activity
    .filter(F.col("max_days_since_end_date") > 140)
    .select(
        "*",
        F.round(F.col("max_days_since_end_date") / F.nullif(F.col("row_count"), F.lit(0)), 3).alias("max_days_since_end_date_per_row_count"),
        F.coalesce(F.col("product"), F.lit("UNKNOWN")).alias("product_or_unknown"),
    )
)

result = (
    enriched
    .select(
        "product",
        "min_days_since_end_date",
        "max_days_since_end_date",
        "row_count",
        "max_days_since_end_date_per_row_count",
        "product_or_unknown",
    )
)
