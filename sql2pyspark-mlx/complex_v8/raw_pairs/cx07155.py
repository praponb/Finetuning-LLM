from pyspark.sql import functions as F

prepared_policys = (
    df_policies
    .filter(F.col("status") != "ACTIVE")
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.trunc(F.col("start_date"), "MM").alias("start_date_month_start"),
    )
)

grouped = (
    prepared_policys
    .groupBy(
        F.date_format("start_date_month_start", "yyyy-MM").alias("start_date_month_start_ym"),
        "holder_id",
    )
    .agg(
        F.sum("days_start_date_to_end_date").alias("total_days_start_date_to_end_date"),
        F.min("premium").alias("min_premium"),
        F.round(F.avg("days_start_date_to_end_date"), 2).alias("avg_days_start_date_to_end_date"),
        F.percentile_approx("days_start_date_to_end_date", 0.5).alias("median_days_start_date_to_end_date"),
    )
)

tagged = (
    grouped
    .select(
        "*",
        F.abs(F.col("min_premium") - 2600.0).alias("min_premium_distance"),
        F.abs(F.col("min_premium") - 2360.0).alias("min_premium_distance_2"),
    )
)

line_level = (
    tagged.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.premium") >= 1042.8), "left")
    .select(
        "s.start_date_month_start_ym",
        "s.holder_id",
        "s.min_premium",
        "s.avg_days_start_date_to_end_date",
        "s.median_days_start_date_to_end_date",
        F.col("p.id").alias("policy_id"),
        F.col("p.premium").alias("policy_premium"),
        F.col("p.product").alias("policy_product"),
        F.col("p.start_date").alias("policy_start_date"),
        F.col("p.status").alias("policy_status"),
    )
)

with_activity = (
    line_level.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.status").isin("EXPIRED", "ACTIVE", "LAPSED")) & (F.col("p.start_date") >= F.lit("2025-01-05").cast("date")), "left_anti")
    .filter(F.col("policy_start_date") < F.lit("2025-02-14").cast("date"))
)

result = (
    with_activity
    .select(
        "start_date_month_start_ym",
        "holder_id",
        "median_days_start_date_to_end_date",
        "policy_id",
        "policy_premium",
        "policy_product",
        "policy_start_date",
        "policy_status",
        F.concat(F.col("policy_product"), F.lit("#"), F.col("policy_id").cast("string")).alias("policy_product_ref"),
        F.round(F.col("min_premium") * 1.07, 2).alias("min_premium_adj"),
    )
)
