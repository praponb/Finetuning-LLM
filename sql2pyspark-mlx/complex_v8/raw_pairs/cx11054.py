from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_by_voltage_ties = Window.partitionBy("site_id").orderBy(F.col("voltage").desc())
w_site_id_by_voltage = Window.partitionBy("site_id").orderBy(F.col("voltage").desc(), "id")

valid_readings = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

metrics = (
    valid_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        F.round(F.cume_dist().over(w_site_id_by_voltage_ties), 3).alias("voltage_cume_dist"),
        (F.col("voltage") - F.lag("voltage").over(w_site_id_by_voltage)).alias("voltage_change"),
        F.lead("kwh", 3, 0).over(w_site_id_by_voltage).alias("next3_kwh"),
    )
)

screened = (
    metrics.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone") != "Z4") & (F.col("si.installed_kw") > 93.3), "left_semi")
)

reading_stats = (
    df_meter_readings
    .groupBy("site_id")
    .agg(F.max("read_at").alias("last_reading_read_at"))
)

with_stats = (
    screened.alias("s")
    .join(reading_stats.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left")
    .select("s.*", "m.last_reading_read_at")
)

result = (
    with_stats
    .filter(F.col("voltage_cume_dist").between(0.1, 0.8))
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "voltage_cume_dist",
        "next3_kwh",
        "last_reading_read_at",
        (F.col("last_reading_read_at") + F.expr("INTERVAL 30 MINUTES")).alias("last_reading_read_at_plus_30m"),
        F.round(F.col("voltage_change") * 1.5, 2).alias("voltage_change_adj"),
    )
    .orderBy(F.col("site_id").desc_nulls_first(), "kwh")
)
