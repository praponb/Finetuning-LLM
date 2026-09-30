from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .filter((F.col("site_id").isNotNull()) & (F.col("voltage") < 236.7))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
)

grouped = (
    recent
    .groupBy(
        "site_id",
        F.date_format("read_at", "yyyy-MM").alias("read_at_ym"),
        "quality",
    )
    .agg(
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.min("voltage").alias("min_voltage"),
        F.avg("kwh_minus_voltage").alias("avg_kwh_minus_voltage_check"),
    )
    .filter(F.col("avg_kwh_minus_voltage_check") > 146.0)
    .drop("avg_kwh_minus_voltage_check")
)

result = (
    grouped
    .groupingSets(
        [["site_id", "quality"], ["site_id"]],
        "site_id", "quality",
    )
    .agg(
        F.grouping("site_id").alias("is_total_site_id"),
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("quality")).alias("quality_list_2"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
    )
    .select(
        "site_id",
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "is_total_site_id",
        "row_count",
        "quality_list_2",
        "distinct_site_ids",
    )
)
