from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), F.col("id").desc())

prepared_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.round(F.avg("kwh"), 2).alias("avg_reading_kwh"))
)

with_stats = (
    prepared_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.avg_reading_kwh")
)

unique_rows = (
    with_stats
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

combined_details_site = (
    unique_rows.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.site_name").isNotNull()), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.quality_slug",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
        F.col("si.installed_kw").alias("site_installed_kw"),
    )
)

without_matches = (
    combined_details_site.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.voltage") >= 215.8) & (F.month(F.col("m.read_at")).isin(1, 3)), "left_anti")
)

result = (
    without_matches
    .filter(F.col("kwh") < 110.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality_slug",
        "site_commissioned_on",
        "site_name",
        F.round(F.col("site_installed_kw") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("site_installed_kw_per_kwh"),
        F.date_trunc("year", F.col("site_commissioned_on")).alias("site_commissioned_on_year"),
    )
    .orderBy("site_installed_kw_per_kwh", F.col("kwh").desc())
)
