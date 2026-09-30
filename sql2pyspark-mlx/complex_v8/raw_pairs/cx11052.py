from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone = Window.partitionBy("zone")
w_zone_by_voltage = Window.partitionBy("zone").orderBy("voltage", "id")
w_zone_read_at_month_by_kwh_fu = Window.partitionBy("zone", F.month("read_at")).orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_zone_by_energy_cost = Window.partitionBy("zone").orderBy("energy_cost", "id")
w_zone_read_at_month_by_read_a = Window.partitionBy("zone", F.date_trunc("month", "read_at")).orderBy(F.col("read_at").asc_nulls_last(), "id")

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
        "r.voltage",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

qualified = (
    joined_ranges.alias("s")
    .join(df_meter_readings.alias("m"), F.col("m.site_id") == F.col("s.site_id"), "left_semi")
)

sequenced = (
    qualified
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "zone",
        "rate",
        "energy_cost",
        F.round(F.col("voltage") - F.avg("voltage").over(w_zone), 2).alias("voltage_vs_zone_avg"),
        F.round(F.cume_dist().over(w_zone_by_voltage), 3).alias("voltage_cume_dist"),
    )
)

windowed = (
    sequenced
    .select(
        "*",
        F.nth_value("voltage", 2).over(w_zone_read_at_month_by_kwh_fu).alias("second_voltage"),
        F.ntile(3).over(w_zone_by_energy_cost).alias("energy_cost_tercile"),
        F.row_number().over(w_zone_read_at_month_by_read_a).alias("seq_no"),
    )
)

result = (
    windowed
    .select(
        "id",
        "read_at",
        "voltage",
        "zone",
        "rate",
        "energy_cost",
        "voltage_cume_dist",
        "second_voltage",
        "energy_cost_tercile",
    )
    .orderBy(F.col("rate").desc(), F.col("voltage_cume_dist").asc_nulls_last())
)
