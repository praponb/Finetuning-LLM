from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id")
w_zone_site_id_by_read_at = Window.partitionBy("zone", "site_id").orderBy("read_at", "id")

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

sequenced = (
    matched
    .select(
        "id",
        "read_at",
        "kwh",
        "energy_cost",
        F.round(F.col("voltage") / F.nullif(F.sum("voltage").over(w_site_id), F.lit(0)), 4).alias("voltage_share_of_site_id"),
        F.lead("energy_cost", 3).over(w_zone_site_id_by_read_at).alias("next3_energy_cost"),
        F.count(F.lit(1)).over(w_site_id).alias("rows_in_site_id"),
        F.round(F.col("energy_cost") / F.nullif(F.sum("energy_cost").over(w_site_id), F.lit(0)), 4).alias("energy_cost_share_of_site_id"),
        F.lead("read_at").over(w_zone_site_id_by_read_at).alias("next_read_at"),
    )
)

scored = (
    sequenced
    .select(
        "id",
        "read_at",
        "energy_cost",
        "next3_energy_cost",
        "rows_in_site_id",
        "energy_cost_share_of_site_id",
        F.round(F.col("rows_in_site_id") / F.nullif(F.col("kwh"), F.lit(0)), 3).alias("rows_in_site_id_per_kwh"),
        F.abs(F.col("kwh") - 43.1).alias("kwh_distance"),
    )
)

screened = (
    scored
    .crossJoin(scored.agg(F.min("rows_in_site_id_per_kwh").alias("overall_min_rows_in_site_id_per_kwh")))
    .filter(F.col("rows_in_site_id_per_kwh") > F.col("overall_min_rows_in_site_id_per_kwh"))
    .drop("overall_min_rows_in_site_id_per_kwh")
)

result = (
    screened
    .filter(F.col("rows_in_site_id_per_kwh") <= 9.0)
    .select(
        "id",
        "read_at",
        "energy_cost",
        "next3_energy_cost",
        "rows_in_site_id",
        "energy_cost_share_of_site_id",
        "kwh_distance",
    )
)
