from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_site_id = Window.partitionBy("zone", "site_id")
w_site_id_by_read_at = Window.partitionBy("site_id").orderBy("read_at", "id")

covered = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "inner",
    )
    .select(
        "r.id",
        "r.site_id",
        "r.read_at",
        "r.kwh",
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

sequenced = (
    covered
    .select(
        "*",
        F.round(F.avg("voltage").over(w_zone_site_id), 2).alias("zone_site_id_avg_voltage"),
        F.ntile(4).over(w_site_id_by_read_at).alias("read_at_quartile"),
    )
)

detail_rows_readings = (
    sequenced.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "inner")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.rate",
        "s.zone_site_id_avg_voltage",
        "s.read_at_quartile",
        F.col("m.id").alias("reading_id"),
        F.col("m.voltage").alias("reading_voltage"),
        F.col("m.kwh").alias("reading_kwh"),
    )
)

result = (
    detail_rows_readings
    .select(
        "id",
        "read_at",
        "voltage",
        "rate",
        "zone_site_id_avg_voltage",
        "read_at_quartile",
        "reading_id",
        "reading_voltage",
    )
    .orderBy(F.col("rate").desc_nulls_first(), "id", "reading_id")
    .limit(5)
)
