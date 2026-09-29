from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_energy_cost_full = Window.orderBy(F.col("energy_cost").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_zone_site_id = Window.partitionBy("zone", "site_id")

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
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

enriched = (
    in_window
    .select(
        "id",
        "site_id",
        "zone",
        "rate",
        "energy_cost",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.lower(F.trim(F.col("zone"))).alias("zone_clean"),
        F.dayofweek(F.col("read_at")).alias("read_at_dayofweek"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "zone",
        "energy_cost",
        "zone_clean",
        "read_at_dayofweek",
        F.nth_value("rate", 2).over(w_all_by_energy_cost_full).alias("second_rate"),
        F.round(F.col("energy_cost") - F.avg("energy_cost").over(w_zone_site_id), 2).alias("energy_cost_vs_zone_site_id_avg"),
        F.sum("rate").over(w_zone_site_id).alias("zone_site_id_sum_rate"),
    )
)

filtered = (
    metrics
    .crossJoin(metrics.agg(F.avg("read_at_dayofweek").alias("overall_avg_read_at_dayofweek")))
    .filter(F.col("read_at_dayofweek") > F.col("overall_avg_read_at_dayofweek"))
    .drop("overall_avg_read_at_dayofweek")
)

result = (
    filtered
    .filter(F.length(F.trim(F.col("zone_clean"))) >= 8)
    .select(
        "id",
        "zone",
        "energy_cost",
        "read_at_dayofweek",
        "zone_site_id_sum_rate",
    )
    .orderBy(F.col("zone").desc())
)
