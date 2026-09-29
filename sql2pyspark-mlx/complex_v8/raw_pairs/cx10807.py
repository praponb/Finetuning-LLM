from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_group_2_by_v = Window.partitionBy("site_id", "quality_group_2").orderBy("voltage")
w_quality_group_2 = Window.partitionBy("quality_group_2")

base = (
    df_meter_readings
    .filter((F.col("voltage") >= 222.3) & (F.col("read_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("quality").isin("BAD", "EST"), "group_a").when(F.col("quality") == "OK", "group_b").otherwise("other").alias("quality_group"),
        F.when(F.col("quality").isin("OK", "EST"), "group_a").when(F.col("quality") == "BAD", "group_b").otherwise("other").alias("quality_group_2"),
    )
)

with_trends = (
    base
    .select(
        "id",
        "site_id",
        "voltage",
        "quality",
        F.rank().over(w_site_id_quality_group_2_by_v).alias("voltage_rank"),
        F.size(F.collect_set("site_id").over(w_quality_group_2)).alias("distinct_site_id_in_quality_group_2"),
        F.max("kwh").over(w_quality_group_2).alias("quality_group_2_max_kwh"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_quality_group_2), 2).alias("voltage_vs_quality_group_2_avg"),
    )
)

summary = (
    with_trends
    .groupBy(
        F.when(F.col("quality_group_2_max_kwh") >= 780.0, "A").when(F.col("quality_group_2_max_kwh") >= 540.0, "B").otherwise("C").alias("quality_group_2_max_kwh_tier"),
        "distinct_site_id_in_quality_group_2",
        "site_id",
    )
    .agg(
        F.sum(F.when(F.col("quality") == "OK", F.col("voltage_vs_quality_group_2_avg")).otherwise(0)).alias("voltage_vs_quality_group_2_avg_quality_ok"),
        F.array_sort(F.collect_set("quality")).alias("quality_list"),
        F.count(F.when(F.col("quality").isin("BAD", "OK"), 1)).alias("n_quality_in_bad_ok"),
        F.sum("voltage_vs_quality_group_2_avg").alias("sum_voltage_vs_quality_group_2_avg_check"),
    )
    .filter(F.col("sum_voltage_vs_quality_group_2_avg_check") > -98.0)
    .drop("sum_voltage_vs_quality_group_2_avg_check")
)

with_details_site = (
    summary.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.quality_group_2_max_kwh_tier",
        "s.distinct_site_id_in_quality_group_2",
        "s.site_id",
        "s.voltage_vs_quality_group_2_avg_quality_ok",
        "s.quality_list",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    with_details_site
    .filter(F.col("site_id") <= 6)
    .select(
        "quality_group_2_max_kwh_tier",
        "distinct_site_id_in_quality_group_2",
        "site_id",
        "quality_list",
        "site_tariff",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("site_commissioned_on")).alias("days_since_site_commissioned_on"),
        F.coalesce(F.col("site_installed_kw"), F.lit(0.0)).alias("site_installed_kw_filled"),
    )
)
