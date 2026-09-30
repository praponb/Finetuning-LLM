from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_rate_full = Window.orderBy("rate", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_zone_site_id_by_voltage = Window.partitionBy("zone", "site_id").orderBy("voltage", "id")
w_zone_read_at_month_by_first = Window.partitionBy("zone", F.month("read_at")).orderBy(F.col("first_rate").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_zone_read_at_month_by_first_2 = Window.partitionBy("zone", F.month("read_at")).orderBy(F.col("first_rate").desc(), "id").rowsBetween(-2, Window.currentRow)
w_zone_by_energy_cost_moving_1 = Window.partitionBy("zone").orderBy("energy_cost", "id").rowsBetween(-1, Window.currentRow)

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

metrics = (
    joined_ranges
    .withColumn("first_rate", F.first("rate").over(w_all_by_rate_full))
    .withColumn("next3_kwh", F.lead("kwh", 3).over(w_zone_site_id_by_voltage))
)

with_windows = (
    metrics
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "zone",
        "rate",
        F.first("rate").over(w_zone_read_at_month_by_first).alias("first_rate_2"),
        F.max("energy_cost").over(w_zone_read_at_month_by_first_2).alias("energy_cost_max_3"),
        F.round(F.avg("rate").over(w_zone_by_energy_cost_moving_1), 2).alias("rate_avg_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "site_id",
        "read_at",
        "zone",
        "first_rate_2",
        F.lower(F.regexp_replace(F.trim(F.col("zone")), " ", "_")).alias("zone_slug"),
    )
    .orderBy(F.col("zone_slug").desc_nulls_first(), "site_id")
)
