from pyspark.sql import functions as F

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

with_details = (
    joined_ranges.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.zone") == "Z2"), "inner")
    .filter(F.col("si.zone") == "Z4")
    .select(
        "s.id",
        "s.site_id",
        "s.read_at",
        "s.voltage",
        "s.zone",
        "s.rate",
        "s.energy_cost",
        "si.site_name",
        F.col("si.zone").alias("site_zone"),
    )
)

grouped = (
    with_details
    .groupBy(
        "site_id",
        "site_zone",
        F.date_trunc("month", "read_at").alias("read_at_month"),
    )
    .agg(
        F.round(F.sum("energy_cost") / F.nullif(F.sum("rate"), F.lit(0)), 3).alias("energy_cost_to_rate_ratio"),
        F.max("read_at").alias("last_read_at"),
        F.count("*").alias("n_rows"),
        F.sum(F.when(F.col("site_zone") != "Z4", 1).otherwise(0)).alias("n_site_zone_z4"),
    )
)

result = (
    grouped
    .select(
        "site_id",
        "site_zone",
        "read_at_month",
        "energy_cost_to_rate_ratio",
        "last_read_at",
        "n_rows",
        F.lower(F.regexp_replace(F.trim(F.col("site_zone")), " ", "_")).alias("site_zone_slug"),
        F.lower(F.trim(F.col("site_zone"))).alias("site_zone_clean"),
    )
    .orderBy(F.col("energy_cost_to_rate_ratio").desc())
)
