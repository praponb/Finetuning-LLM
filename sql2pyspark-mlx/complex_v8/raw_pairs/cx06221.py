from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_voltage = Window.partitionBy("site_id").orderBy("voltage", "id")
w_site_id = Window.partitionBy("site_id")
w_dedup = Window.partitionBy("site_zone", "site_id").orderBy(F.col("voltage_decile").desc(), F.col("id").desc())

filtered_readings = (
    df_meter_readings
    .filter((F.col("quality") == "BAD") & (F.col("read_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.to_date(F.col("read_at")).alias("read_at_date"),
        F.date_format(F.col("read_at"), "yyyy-MM-dd").alias("read_at_label"),
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

windowed = (
    filtered_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality_slug",
        F.ntile(10).over(w_site_id_by_voltage).alias("voltage_decile"),
        F.max("voltage").over(w_site_id).alias("site_id_max_voltage"),
        F.ntile(5).over(w_site_id_by_voltage).alias("voltage_quintile"),
        F.round(F.cume_dist().over(w_site_id_by_voltage), 3).alias("voltage_cume_dist"),
    )
)

with_details_site = (
    windowed.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality_slug",
        "s.voltage_decile",
        "s.site_id_max_voltage",
        F.col("si.installed_kw").alias("site_installed_kw"),
        F.col("si.zone").alias("site_zone"),
    )
)

latest_per_key = (
    with_details_site
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .filter(F.col("quality_slug").isNotNull())
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "voltage_decile",
        "site_id_max_voltage",
        "site_installed_kw",
        "site_zone",
        F.upper(F.trim(F.col("quality_slug"))).alias("quality_slug_clean"),
        (F.col("kwh") - F.col("site_id_max_voltage")).alias("kwh_minus_site_id_max_voltage"),
    )
)
