from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_last_reading_read_at = Window.orderBy("last_reading_read_at", "id")
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_site_zone_by_max_reading_kwh = Window.partitionBy("site_zone").orderBy("max_reading_kwh_change", "id")
w_site_id = Window.partitionBy("site_id")
w_site_zone_by_max_reading_kwh_2 = Window.partitionBy("site_zone").orderBy("max_reading_kwh_change", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

with_parent = (
    filtered.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") >= 93.3), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.quality",
        "s.days_since_read_at",
        F.col("si.tariff").alias("site_tariff"),
        F.col("si.zone").alias("site_zone"),
        "si.site_name",
    )
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(
        F.max("read_at").alias("last_reading_read_at"),
        F.max("kwh").alias("max_reading_kwh"),
    )
)

profiled = (
    with_parent.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.last_reading_read_at", "m.max_reading_kwh")
)

sequenced = (
    profiled
    .withColumn("max_reading_kwh_change", F.col("max_reading_kwh") - F.lag("max_reading_kwh").over(w_all_by_last_reading_read_at))
    .withColumn("max_reading_kwh_share_of_site_id_quality", F.round(F.col("max_reading_kwh") / F.nullif(F.sum("max_reading_kwh").over(w_site_id_quality), F.lit(0)), 4))
)

with_details_site = (
    sequenced.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select("s.*", F.col("si.zone").alias("site_zone_2"))
)

with_trends = (
    with_details_site
    .select(
        "*",
        F.dense_rank().over(w_site_zone_by_max_reading_kwh).alias("max_reading_kwh_change_dense_rank"),
        F.rank().over(w_site_zone_by_max_reading_kwh).alias("max_reading_kwh_change_rank"),
        F.ntile(10).over(w_site_zone_by_max_reading_kwh).alias("max_reading_kwh_change_decile"),
        F.size(F.collect_set("site_zone_2").over(w_site_id)).alias("distinct_site_zone_2_in_site_id"),
        F.sum("kwh").over(w_site_zone_by_max_reading_kwh_2).alias("running_kwh"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "site_id",
        "quality",
        "days_since_read_at",
        "site_zone",
        "site_name",
        "last_reading_read_at",
        "max_reading_kwh",
        "max_reading_kwh_change",
        "max_reading_kwh_share_of_site_id_quality",
        "max_reading_kwh_change_rank",
        "running_kwh",
        F.length(F.trim(F.col("site_zone_2"))).alias("site_zone_2_len"),
        F.concat(F.col("site_tariff"), F.lit("#"), F.col("site_id").cast("string")).alias("site_tariff_ref"),
    )
)
