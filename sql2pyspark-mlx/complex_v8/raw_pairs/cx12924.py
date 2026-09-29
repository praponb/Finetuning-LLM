from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_all_by_voltage_moving_3 = Window.orderBy("voltage", "id").rowsBetween(-3, Window.currentRow)
w_all_by_voltage_ties = Window.orderBy("voltage")

base_readings = (
    df_meter_readings
    .filter((F.col("site_id") >= 6) & (F.col("read_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd HH:00").alias("read_at_label"),
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label_2"),
    )
)

with_windows = (
    base_readings
    .select(
        "*",
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.round(F.avg("voltage").over(w_all_by_voltage_moving_3), 2).alias("voltage_avg_4"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_site_id), 2).alias("kwh_vs_site_id_avg"),
        F.round(F.avg("kwh").over(w_site_id), 2).alias("site_id_avg_kwh"),
        F.rank().over(w_all_by_voltage_ties).alias("voltage_rank"),
    )
)

enriched = (
    with_windows.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.quality",
        "s.read_at_label",
        "s.read_at_label_2",
        "s.voltage_avg_4",
        "s.kwh_vs_site_id_avg",
        "s.site_id_avg_kwh",
        "s.voltage_rank",
        F.col("si.tariff").alias("site_tariff"),
    )
)

result = (
    enriched
    .groupBy("site_id", "site_tariff")
    .pivot("quality", ["OK", "BAD"])
    .agg(F.sum("kwh_vs_site_id_avg").alias("total"), F.count("kwh_vs_site_id_avg").alias("n"))
    .orderBy("site_id", "site_tariff")
)
