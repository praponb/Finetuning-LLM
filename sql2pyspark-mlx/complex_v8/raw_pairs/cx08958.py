from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_zone_zone_group_by_read_at = Window.partitionBy("zone", "zone_group").orderBy("read_at", "id")

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

enriched = (
    joined_ranges
    .select(
        "id",
        "read_at",
        "kwh",
        "zone",
        "energy_cost",
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.when(F.col("zone").isin("Z4", "Z3"), "group_a").when(F.col("zone") == "Z2", "group_b").otherwise("other").alias("zone_group"),
    )
)

with_trends = (
    enriched
    .select(
        "id",
        "read_at",
        "kwh",
        "energy_cost",
        "kwh_filled",
        F.lead("energy_cost", 1, 0).over(w_zone_zone_group_by_read_at).alias("next_energy_cost"),
        (F.col("energy_cost") - F.lag("energy_cost").over(w_zone_zone_group_by_read_at)).alias("energy_cost_change"),
    )
)

result = (
    with_trends
    .filter(F.col("kwh") >= 48.2)
    .select(
        "id",
        "read_at",
        "energy_cost",
        "kwh_filled",
        "energy_cost_change",
        F.abs(F.col("next_energy_cost") - 315.0).alias("next_energy_cost_distance"),
    )
    .orderBy(F.col("energy_cost_change").asc(), "next_energy_cost_distance")
)
