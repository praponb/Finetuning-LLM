from pyspark.sql import functions as F

cleaned = (
    df_policies
    .filter(F.col("start_date") >= F.lit("2024-10-22").cast("date"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.date_add(F.col("end_date"), 30).alias("end_date_plus_30d"),
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

with_details = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.status_slug",
        "s.end_date_plus_30d",
        "s.days_start_date_to_end_date",
        F.col("p.segment").alias("holder_segment"),
        "p.holder_name",
    )
)

screened = (
    with_details.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.holder_id")) & (F.col("p.end_date") >= F.lit("2026-01-23").cast("date")), "left_anti")
    .filter(F.col("holder_name").isNotNull())
)

status_stats = (
    screened
    .groupBy("status")
    .agg(F.avg("days_start_date_to_end_date").alias("avg_days_start_date_to_end_date"))
)

outliers = (
    screened.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.days_start_date_to_end_date") > F.col("g.avg_days_start_date_to_end_date"))
    .select(
        "s.*",
        "g.avg_days_start_date_to_end_date",
        F.round(F.col("s.days_start_date_to_end_date") - F.col("g.avg_days_start_date_to_end_date"), 2).alias("days_start_date_to_end_date_minus_avg"),
    )
)

result = (
    outliers
    .groupBy("holder_id")
    .pivot("status", ["ACTIVE", "EXPIRED"])
    .agg(F.min("premium").alias("min_v"), F.sum("premium").alias("sum_v"))
    .orderBy("holder_id")
)
