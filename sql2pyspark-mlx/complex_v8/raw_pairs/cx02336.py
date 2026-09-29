from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_site_installed_kw = Window.partitionBy("quality").orderBy("site_installed_kw")
w_site_commissioned_on_year_re = Window.partitionBy(F.year("site_commissioned_on"), F.to_date("read_at_year"))
w_all_by_read_at_year_ties = Window.orderBy(F.col("read_at_year").desc())

base_readings = (
    df_meter_readings
    .filter((F.col("site_id") >= 2) | (F.col("read_at") >= F.lit("2025-01-19 09:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.abs(F.col("kwh") - 81.9).alias("kwh_distance"),
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
        F.date_trunc("year", F.col("read_at")).alias("read_at_year"),
    )
)

combined_details_site = (
    base_readings.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.kwh_distance",
        "s.read_at_year",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

annotated = (
    combined_details_site
    .select(
        "*",
        F.length(F.trim(F.col("site_tariff"))).alias("site_tariff_len"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.substring(F.trim(F.col("site_tariff")), 1, 3).alias("site_tariff_prefix"),
    )
)

with_trends = (
    annotated
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "read_at_year",
        "site_commissioned_on",
        "site_installed_kw",
        "site_tariff_len",
        "kwh_filled",
        F.round(F.cume_dist().over(w_quality_by_site_installed_kw), 3).alias("site_installed_kw_cume_dist"),
        F.max("kwh_distance").over(w_site_commissioned_on_year_re).alias("site_commissioned_on_year_read_at_year_to_date_m"),
        F.count(F.lit(1)).over(w_site_commissioned_on_year_re).alias("rows_in_site_commissioned_on_year_read_at_year_t"),
        F.round(F.cume_dist().over(w_all_by_read_at_year_ties), 3).alias("read_at_year_cume_dist"),
    )
)

enriched = (
    with_trends
    .withColumn("site_commissioned_on_label", F.date_format(F.col("site_commissioned_on"), "yyyy-MM"))
)

result = (
    enriched
    .filter(F.col("site_tariff_len").between(3, 11))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "read_at_year",
        "site_installed_kw",
        "kwh_filled",
        "site_installed_kw_cume_dist",
        "site_commissioned_on_year_read_at_year_to_date_m",
        "rows_in_site_commissioned_on_year_read_at_year_t",
        "read_at_year_cume_dist",
        "site_commissioned_on_label",
    )
)
