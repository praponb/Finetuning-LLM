from pyspark.sql import functions as F

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

labeled = (
    matched
    .filter(F.col("energy_cost").between(270.0, 585.0))
    .select(
        "*",
        F.coalesce(F.col("zone"), F.lit("UNKNOWN")).alias("zone_or_unknown"),
        F.round(F.col("rate") * 1.5, 2).alias("rate_adj"),
        F.when(F.col("zone").isin("Z4", "Z3"), "group_a").when(F.col("zone") == "Z2", "group_b").otherwise("other").alias("zone_group"),
    )
)

with_details_site = (
    labeled.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "left")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.kwh",
        "s.voltage",
        "s.zone",
        "s.rate",
        "s.energy_cost",
        "s.zone_or_unknown",
        "s.rate_adj",
        "s.zone_group",
        F.col("si.commissioned_on").alias("site_commissioned_on"),
    )
)

joined_site = (
    with_details_site.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.commissioned_on").between(F.lit("2024-06-27").cast("date"), F.lit("2025-05-07").cast("date")))
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.zone",
        "s.rate",
        "s.energy_cost",
        "s.zone_group",
        F.col("si.commissioned_on").alias("site_commissioned_on_2"),
        "si.site_name",
    )
)

result = (
    joined_site
    .select(
        "id",
        "site_id",
        "read_at",
        "zone",
        "site_name",
        F.round(F.col("energy_cost") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("energy_cost_per_voltage"),
    )
    .orderBy(F.col("read_at").asc_nulls_last(), F.col("site_name").desc())
)
