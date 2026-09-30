from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_site_id = Window.partitionBy("zone", "site_id")
w_zone_by_voltage = Window.partitionBy("zone").orderBy("voltage", "id")
w_zone_read_at_to_date_by_volt = Window.partitionBy("zone", F.to_date("read_at")).orderBy("voltage", "id").rowsBetween(-6, Window.currentRow)
w_zone_by_voltage_full = Window.partitionBy("zone").orderBy("voltage", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

matched = (
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

windowed = (
    matched
    .select(
        "*",
        F.round(F.avg("energy_cost").over(w_zone_site_id), 2).alias("zone_site_id_avg_energy_cost"),
        (F.col("voltage") - F.lag("voltage").over(w_zone_by_voltage)).alias("voltage_change"),
        F.sum("energy_cost").over(w_zone_read_at_to_date_by_volt).alias("energy_cost_sum_7"),
        F.first("rate").over(w_zone_by_voltage_full).alias("first_rate"),
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_zone_site_id), F.lit(0)), 4).alias("voltage_share_of_zone_site_id"),
    )
)

labeled = (
    windowed
    .select(
        "*",
        F.round(F.col("rate") / F.nullif(F.col("energy_cost_sum_7"), F.lit(0)), 3).alias("rate_per_energy_cost_sum_7"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
        F.when(F.col("voltage_change") >= 73.5, "A").when(F.col("voltage_change") >= -73.5, "B").otherwise("C").alias("voltage_change_tier"),
    )
)

result = (
    labeled
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "zone",
        "energy_cost",
        "zone_site_id_avg_energy_cost",
        "voltage_change",
        "energy_cost_sum_7",
        "rate_per_energy_cost_sum_7",
        "days_since_read_at",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
    )
    .orderBy("voltage", F.col("read_at").desc())
)
