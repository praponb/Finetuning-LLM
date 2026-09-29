from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_kwh = Window.partitionBy("site_id").orderBy(F.col("kwh").desc(), "id")
w_quality_site_id = Window.partitionBy("quality", "site_id")

base_readings = (
    df_meter_readings
    .filter(F.col("kwh") <= 109.3)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.to_date(F.col("read_at")).alias("read_at_date"),
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
    )
)

quality_stats = (
    base_readings
    .groupBy("quality")
    .agg(F.percentile_approx("kwh_minus_voltage", 0.5).alias("median_kwh_minus_voltage"))
)

outliers = (
    base_readings.alias("s")
    .join(quality_stats.alias("g"), F.col("g.quality") == F.col("s.quality"), "inner")
    .filter(F.col("s.kwh_minus_voltage") >= F.col("g.median_kwh_minus_voltage"))
    .select(
        "s.*",
        "g.median_kwh_minus_voltage",
        F.round(F.col("s.kwh_minus_voltage") - F.col("g.median_kwh_minus_voltage"), 2).alias("kwh_minus_voltage_minus_median"),
    )
)

tagged = (
    outliers
    .select(
        "id",
        "site_id",
        "kwh",
        "quality",
        "median_kwh_minus_voltage",
        "kwh_minus_voltage_minus_median",
        F.coalesce(F.col("median_kwh_minus_voltage"), F.lit(0.0)).alias("median_kwh_minus_voltage_filled"),
        F.greatest(F.col("kwh_minus_voltage"), F.col("kwh")).alias("greatest_kwh_minus_voltage_kwh"),
    )
)

windowed = (
    tagged
    .select(
        "*",
        F.lead("kwh").over(w_site_id_by_kwh).alias("next_kwh"),
        F.lag("median_kwh_minus_voltage").over(w_site_id_by_kwh).alias("prev_median_kwh_minus_voltage"),
        F.round(F.avg("kwh").over(w_quality_site_id), 2).alias("quality_site_id_avg_kwh"),
    )
)

scored = (
    windowed
    .withColumn("quality_ref", F.concat(F.col("quality"), F.lit("#"), F.col("id").cast("string")))
)

result = (
    scored
    .filter(F.col("quality_site_id_avg_kwh") > 48.0)
    .select(
        "id",
        "quality",
        "median_kwh_minus_voltage",
        "kwh_minus_voltage_minus_median",
        "prev_median_kwh_minus_voltage",
        "quality_site_id_avg_kwh",
        "quality_ref",
    )
)
