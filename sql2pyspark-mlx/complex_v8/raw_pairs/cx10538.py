from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_zone_reading_count_by_l = Window.partitionBy("site_zone", "reading_count").orderBy("last_reading_read_at", "id", "reading_id").rowsBetween(-1, -1)
w_reading_id_by_kwh = Window.partitionBy("reading_id").orderBy(F.col("kwh").desc(), "id", "reading_id")
w_site_zone_site_tariff = Window.partitionBy("site_zone", "site_tariff")
w_site_zone_reading_count_by_l_2 = Window.partitionBy("site_zone", "reading_count").orderBy("last_reading_read_at", "id", "reading_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        F.abs(F.col("kwh") - 83.8).alias("kwh_distance"),
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
        F.concat(F.col("quality"), F.lit("#"), F.col("site_id").cast("string")).alias("quality_ref"),
    )
)

with_children_readings = (
    recent_readings.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality_ref",
        F.col("m.id").alias("reading_id"),
        F.col("m.kwh").alias("reading_kwh"),
        F.col("m.read_at").alias("reading_read_at"),
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.count("*").alias("reading_count"),
        F.max("read_at").alias("last_reading_read_at_2"),
    )
)

with_counts = (
    with_children_readings.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select(
        "s.*",
        "m.last_reading_read_at",
        F.coalesce(F.col("m.reading_count"), F.lit(0)).alias("reading_count"),
        "m.last_reading_read_at_2",
    )
)

combined_details = (
    with_counts.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .join(df_meter_readings.alias("m"), F.col("m.id") == F.col("s.reading_id"), "left")
    .select(
        "s.*",
        F.col("si.zone").alias("site_zone"),
        F.col("si.commissioned_on").alias("site_commissioned_on"),
        "si.site_name",
        F.col("m.read_at").alias("reading_read_at_2"),
    )
)

with_details = (
    combined_details.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff").isNotNull()), "left")
    .select("s.*", F.col("si.tariff").alias("site_tariff"))
)

ranked = (
    with_details
    .select(
        "*",
        F.max("voltage").over(w_site_zone_reading_count_by_l).alias("voltage_max_1_prior"),
        F.dense_rank().over(w_reading_id_by_kwh).alias("kwh_dense_rank"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_zone_site_tariff), F.lit(0)), 4).alias("voltage_share_of_site_zone_site_tariff"),
        F.sum("reading_count").over(w_site_zone_reading_count_by_l_2).alias("running_reading_count"),
        F.size(F.collect_set("reading_id").over(w_site_zone_site_tariff)).alias("distinct_reading_id_in_site_zone_site_tariff"),
    )
)

result = (
    ranked
    .select(
        "id",
        "site_id",
        "kwh",
        "quality_ref",
        "reading_id",
        "reading_kwh",
        "reading_read_at",
        "last_reading_read_at",
        "last_reading_read_at_2",
        "site_zone",
        "site_commissioned_on",
        "site_name",
        "reading_read_at_2",
        "site_tariff",
        "voltage_max_1_prior",
        "kwh_dense_rank",
        "voltage_share_of_site_zone_site_tariff",
        "running_reading_count",
        "distinct_reading_id_in_site_zone_site_tariff",
        F.concat(F.col("site_zone"), F.lit("#"), F.col("kwh_dense_rank").cast("string")).alias("site_zone_ref"),
    )
    .orderBy(
        F.col("kwh").asc(),
        "last_reading_read_at",
        "id",
        "reading_id",
    )
    .limit(20)
)
