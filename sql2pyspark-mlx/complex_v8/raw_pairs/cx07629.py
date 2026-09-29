from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_read_at_year_by_read = Window.partitionBy("quality", F.year("read_at")).orderBy("read_at", "id")
w_read_at_month = Window.partitionBy(F.month("read_at"))

cleaned = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.row_number().over(w_quality_read_at_year_by_read).alias("seq_no"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_read_at_month), 2).alias("kwh_vs_read_at_month_avg"),
        F.size(F.collect_set("quality").over(w_read_at_month)).alias("distinct_quality_in_read_at_month"),
    )
)

screened = (
    with_windows.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") >= 221.4), "left_anti")
)

with_details_site = (
    screened.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone").isNotNull())
    .select("s.*", "si.site_name", F.col("si.zone").alias("site_zone"))
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.count("*").alias("reading_count"),
        F.min("read_at").alias("first_reading_read_at"),
    )
)

with_counts = (
    with_details_site.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.first_reading_read_at",
    )
)

result = (
    with_counts
    .select(
        "id",
        F.col("voltage").alias("m_voltage"),
        F.col("kwh_vs_read_at_month_avg").alias("m_kwh_vs_read_at_month_avg"),
        F.col("kwh").alias("m_kwh"),
    )
    .unpivot(
        ids=["id"],
        values=["m_voltage", "m_kwh_vs_read_at_month_avg", "m_kwh"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
