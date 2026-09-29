from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_read_at_to_date = Window.partitionBy("site_id", F.to_date("read_at"))
w_read_at_year_by_voltage_runn = Window.partitionBy(F.year("read_at")).orderBy(F.col("voltage").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_qualitys_cnt = Window.partitionBy("distinct_qualitys", "cnt")
w_cnt_read_at_week_month_to_da = Window.partitionBy("cnt", F.to_date("read_at_week_month")).orderBy("voltage_vs_site_id_read_at_to_date_avg_quality_i", "read_at_week_month", "read_at_day", "read_at_month_ym").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_readings = (
    df_meter_readings
    .filter(F.col("kwh").between(26.9, 106.5))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.date_trunc("month", F.col("read_at")).alias("read_at_month"),
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
        F.date_trunc("week", F.col("read_at")).alias("read_at_week"),
    )
)

screened = (
    scoped_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.count("*").alias("reading_count_2"),
    )
)

with_stats = (
    screened.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        F.coalesce(F.col("m.reading_count_2"), F.lit(0)).alias("reading_count_2"),
    )
)

combined_details = (
    with_stats.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.quality",
        "s.read_at_month",
        "s.read_at_week",
        "s.reading_count",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

metrics = (
    combined_details
    .select(
        "id",
        "read_at",
        "quality",
        "read_at_month",
        "read_at_week",
        "reading_count",
        F.round(F.col("reading_count") - F.avg("reading_count").over(w_site_id_read_at_to_date), 2).alias("reading_count_vs_site_id_read_at_to_date_avg"),
        F.first("read_at_week", ignorenulls=True).over(w_read_at_year_by_voltage_runn).alias("first_known_read_at_week"),
        F.size(F.collect_set("quality").over(w_site_id_read_at_to_date)).alias("distinct_quality_in_site_id_read_at_to_date"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id_read_at_to_date), F.lit(0)), 4).alias("voltage_share_of_site_id_read_at_to_date"),
        F.round(F.col("voltage") - F.avg("voltage").over(w_site_id_read_at_to_date), 2).alias("voltage_vs_site_id_read_at_to_date_avg"),
        F.round(F.col("site_installed_kw") - F.avg("site_installed_kw").over(w_site_id_read_at_to_date), 2).alias("site_installed_kw_vs_site_id_read_at_to_date_avg"),
    )
)

summary = (
    metrics
    .groupBy(
        F.date_trunc("month", "read_at_week").alias("read_at_week_month"),
        F.to_date("read_at").alias("read_at_day"),
        F.date_format("read_at_month", "yyyy-MM").alias("read_at_month_ym"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("reading_count"), 2).alias("stddev_reading_count"),
        F.sum(F.when(F.col("quality").isNotNull(), F.col("voltage_vs_site_id_read_at_to_date_avg")).otherwise(0)).alias("voltage_vs_site_id_read_at_to_date_avg_quality_i"),
        F.countDistinct("quality").alias("distinct_qualitys"),
        F.round(F.sum("site_installed_kw_vs_site_id_read_at_to_date_avg"), 2).alias("total_site_installed_kw_vs_site_id_read_at_to_da"),
    )
)

with_trends = (
    summary
    .select(
        "*",
        F.round(F.avg("distinct_qualitys").over(w_distinct_qualitys_cnt), 2).alias("distinct_qualitys_cnt_avg_distinct_qualitys"),
        F.min("stddev_reading_count").over(w_cnt_read_at_week_month_to_da).alias("min_stddev_reading_count_so_far"),
        F.round(F.col("distinct_qualitys") - F.avg("distinct_qualitys").over(w_distinct_qualitys_cnt), 2).alias("distinct_qualitys_vs_distinct_qualitys_cnt_avg"),
    )
)

result = (
    with_trends
    .select(
        "read_at_week_month",
        "read_at_month_ym",
        "cnt",
        "stddev_reading_count",
        "distinct_qualitys",
        "total_site_installed_kw_vs_site_id_read_at_to_da",
        "distinct_qualitys_cnt_avg_distinct_qualitys",
        "min_stddev_reading_count_so_far",
        F.concat(F.col("read_at_month_ym"), F.lit("#"), F.col("cnt").cast("string")).alias("read_at_month_ym_ref"),
        F.date_format(F.col("read_at_day"), "yyyy-MM-dd").alias("read_at_day_label"),
    )
    .orderBy("read_at_month_ym", F.col("min_stddev_reading_count_so_far").desc())
)
