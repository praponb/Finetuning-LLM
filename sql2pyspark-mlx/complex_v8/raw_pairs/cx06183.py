from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_quality_by_kwh_runni = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id = Window.partitionBy("site_id")
w_site_id_quality_by_kwh_ties = Window.partitionBy("site_id", "quality").orderBy(F.col("kwh").desc())

recent = (
    df_meter_readings
    .filter((F.col("site_id") >= 8) & (F.col("kwh") >= 29.2))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
)

screened = (
    recent.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.quality").isNotNull()), "left_semi")
    .filter(F.col("kwh_filled") < 108.0)
)

with_windows = (
    screened
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "quality_slug",
        "kwh_filled",
        F.min("voltage").over(w_site_id_quality_by_kwh_runni).alias("min_voltage_so_far"),
        F.size(F.collect_set("quality").over(w_site_id)).alias("distinct_quality_in_site_id"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id), F.lit(0)), 4).alias("voltage_share_of_site_id"),
        F.round(F.cume_dist().over(w_site_id_quality_by_kwh_ties), 3).alias("kwh_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "site_id",
        "voltage",
        "min_voltage_so_far",
        "distinct_quality_in_site_id",
    )
    .orderBy(
        F.col("distinct_quality_in_site_id").desc(),
        F.col("voltage").desc(),
        "id",
    )
    .limit(5)
)
