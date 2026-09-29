from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_tariff_quality = Window.partitionBy("site_tariff", "quality")
w_site_id_by_voltage_bucket_pr = Window.partitionBy("site_id").orderBy(F.col("voltage_bucket").desc(), "id").rowsBetween(-1, -1)
w_site_id_site_tariff_by_site = Window.partitionBy("site_id", "site_tariff").orderBy("site_commissioned_on", "id")
w_all_by_kwh_running = Window.orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id = Window.partitionBy("site_id")
w_site_tariff = Window.partitionBy("site_tariff")
w_site_commissioned_on_year_by = Window.partitionBy(F.year("site_commissioned_on")).orderBy("kwh", "id")
w_quality_or_unknown_by_kwh = Window.partitionBy("quality_or_unknown").orderBy(F.col("kwh").desc(), "id")
w_site_commissioned_on_year_by_2 = Window.partitionBy(F.year("site_commissioned_on")).orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        (F.floor(F.col("voltage") / 5) * 5).alias("voltage_bucket"),
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

combined_details = (
    recent_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.kwh",
        "s.quality",
        "s.voltage_bucket",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.tariff").alias("site_tariff"),
    )
)

metrics = (
    combined_details
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "voltage_bucket",
        "site_commissioned_on",
        "site_tariff",
        F.size(F.collect_set("site_id").over(w_site_tariff_quality)).alias("distinct_site_id_in_site_tariff_quality"),
        F.round(F.avg("voltage_bucket").over(w_site_id_by_voltage_bucket_pr), 2).alias("voltage_bucket_avg_1_prior"),
        F.round(F.avg("voltage_bucket").over(w_site_tariff_quality), 2).alias("site_tariff_quality_avg_voltage_bucket"),
        F.lead("site_commissioned_on", 3).over(w_site_id_site_tariff_by_site).alias("next3_site_commissioned_on"),
        F.count(F.lit(1)).over(w_all_by_kwh_running).alias("running_count"),
    )
)

derived = (
    metrics
    .select(
        "*",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

metrics_2 = (
    derived
    .select(
        "*",
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.max("kwh").over(w_site_id).alias("site_id_max_kwh"),
        F.size(F.collect_set("quality_or_unknown").over(w_site_id)).alias("distinct_quality_or_unknown_in_site_id"),
        F.round(F.avg("running_count").over(w_site_tariff), 2).alias("site_tariff_avg_running_count"),
        F.lead("voltage_bucket_avg_1_prior", 1, 0).over(w_site_commissioned_on_year_by).alias("next_voltage_bucket_avg_1_prior"),
        F.ntile(3).over(w_quality_or_unknown_by_kwh).alias("kwh_tercile"),
        F.last("voltage_bucket_avg_1_prior", ignorenulls=True).over(w_site_commissioned_on_year_by_2).alias("last_known_voltage_bucket_avg_1_prior"),
    )
)

result = (
    metrics_2
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "voltage_bucket",
        "site_commissioned_on",
        "site_tariff",
        "distinct_site_id_in_site_tariff_quality",
        "site_tariff_quality_avg_voltage_bucket",
        "running_count",
        "rows_in_site_id",
        F.round(F.col("kwh") / F.nullif(F.col("rows_in_site_id"), F.lit(0)), 3).alias("kwh_per_rows_in_site_id"),
        F.lower(F.trim(F.col("quality"))).alias("quality_clean"),
    )
    .orderBy(F.col("voltage_bucket").desc(), "running_count")
)
