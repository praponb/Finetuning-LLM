from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_read_at_year_site_id = Window.partitionBy("read_at_year", "site_id")
w_site_id_zone_by_energy_cost = Window.partitionBy("site_id", "zone").orderBy("energy_cost", "id")

covered = (
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

annotated = (
    covered
    .select(
        "*",
        F.year(F.col("read_at")).alias("read_at_year"),
        F.round(F.col("voltage") / F.nullif(F.col("energy_cost"), F.lit(0)), 3).alias("voltage_per_energy_cost"),
    )
)

filtered = (
    annotated.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.installed_kw") > 96.9) & (F.upper(F.trim(F.col("si.site_name"))).like("D%")), "left_anti")
)

ranked = (
    filtered
    .select(
        "id",
        "read_at",
        "kwh",
        "rate",
        "energy_cost",
        "read_at_year",
        "voltage_per_energy_cost",
        F.min("read_at_year").over(w_read_at_year_site_id).alias("read_at_year_site_id_min_read_at_year"),
        F.size(F.collect_set("zone").over(w_read_at_year_site_id)).alias("distinct_zone_in_read_at_year_site_id"),
        F.round(F.cume_dist().over(w_site_id_zone_by_energy_cost), 3).alias("energy_cost_cume_dist"),
    )
)

result = (
    ranked
    .select(
        "id",
        "rate",
        "energy_cost",
        "read_at_year",
        "read_at_year_site_id_min_read_at_year",
        "distinct_zone_in_read_at_year_site_id",
        "energy_cost_cume_dist",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
    .orderBy(F.col("read_at_year").desc())
)
