from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("site_id").orderBy(F.col("energy_cost").desc(), F.col("id").desc())

joined_ranges = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "left",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.min("read_at").alias("first_reading_read_at"),
        F.max("read_at").alias("last_reading_read_at"),
    )
)

with_stats = (
    joined_ranges.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.first_reading_read_at", "m.last_reading_read_at")
)

enriched = (
    with_stats
    .filter(F.col("zone") == "Z2")
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "zone",
        "rate",
        "energy_cost",
        "first_reading_read_at",
        F.substring(F.trim(F.col("quality")), 1, 3).alias("quality_prefix"),
        F.concat_ws(": ", F.col("zone"), F.col("quality")).alias("zone_quality_label"),
        F.concat_ws(" / ", F.col("quality"), F.col("zone")).alias("quality_zone_label"),
    )
)

latest = (
    enriched
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "read_at",
        "quality",
        "zone",
        "rate",
        "energy_cost",
        "quality_prefix",
        "zone_quality_label",
        F.coalesce(F.col("energy_cost"), F.lit(0.0)).alias("energy_cost_filled"),
        F.length(F.trim(F.col("quality_prefix"))).alias("quality_prefix_len"),
    )
    .orderBy(F.col("id").desc(), "quality")
)
