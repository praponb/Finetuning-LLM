from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id_zone = Window.partitionBy("site_id", "zone")
w_all_by_read_at = Window.orderBy("read_at", "id")

in_window = (
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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

with_trends = (
    in_window
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "zone",
        F.round(F.avg("kwh").over(w_site_id_zone), 2).alias("site_id_zone_avg_kwh"),
        F.lead("kwh", 3, 0).over(w_all_by_read_at).alias("next3_kwh"),
        F.round(F.col("rate") - F.avg("rate").over(w_site_id_zone), 2).alias("rate_vs_site_id_zone_avg"),
    )
)

with_activity = (
    with_trends.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") >= F.lit("2025-01-19 07:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("rate_vs_site_id_zone_avg").between(-5.6, 5.6))
)

result = (
    with_activity
    .select("id", "voltage", "kwh")
    .unpivot(
        ids=["id"],
        values=["voltage", "kwh"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
