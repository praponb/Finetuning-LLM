from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_or_unknown_site_id_b = Window.partitionBy("quality_or_unknown", "site_id").orderBy("read_at", "id")
w_all_by_voltage_running = Window.orderBy(F.col("voltage").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_quality_or_unknown = Window.partitionBy("quality_or_unknown")

cleaned_readings = (
    df_meter_readings
    .filter(F.col("quality") == "OK")
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

sequenced = (
    cleaned_readings
    .select(
        "*",
        F.rank().over(w_quality_or_unknown_site_id_b).alias("read_at_rank"),
        F.first("kwh", ignorenulls=True).over(w_all_by_voltage_running).alias("first_known_kwh"),
        F.round(F.col("kwh") - F.avg("kwh").over(w_quality_or_unknown), 2).alias("kwh_vs_quality_or_unknown_avg"),
    )
)

combined_details = (
    sequenced.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.voltage",
        "s.quality_prefix",
        "s.read_at_label",
        "s.read_at_rank",
        "s.kwh_vs_quality_or_unknown_avg",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

result = (
    combined_details
    .groupBy("read_at_rank", "site_commissioned_on")
    .pivot("site_zone", ["Z4", "Z3", "Z2"])
    .agg(F.min("kwh_vs_quality_or_unknown_avg"))
    .orderBy("read_at_rank", "site_commissioned_on")
)
