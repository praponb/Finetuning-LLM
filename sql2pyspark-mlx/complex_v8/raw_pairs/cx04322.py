from pyspark.sql import functions as F

matched = (
    df_meter_readings.alias("r")
    .join(df_sites.alias("st"), F.col("st.id") == F.col("r.site_id"), "inner")
    .join(
        df_tariff_windows.alias("tw"),
        (F.col("tw.tariff") == F.col("st.tariff"))
        & (F.col("r.read_at") >= F.col("tw.valid_from"))
        & (F.col("r.read_at") < F.col("tw.valid_to")),
        "inner",
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

site_id_stats = (
    matched
    .groupBy("site_id")
    .agg(F.max("voltage").alias("max_voltage"))
)

above_average = (
    matched.alias("s")
    .join(site_id_stats.alias("g"), F.col("g.site_id") == F.col("s.site_id"), "inner")
    .filter(F.col("s.voltage") == F.col("g.max_voltage"))
    .select(
        "s.*",
        "g.max_voltage",
        F.round(F.col("s.voltage") - F.col("g.max_voltage"), 2).alias("voltage_minus_max"),
    )
)

stats = (
    above_average
    .filter(F.col("voltage_minus_max") >= -122.5)
    .groupBy(F.to_date("read_at").alias("read_at_day"), "site_id")
    .agg(
        F.sum(F.when(F.col("zone") == "Z3", 1).otherwise(0)).alias("n_zone_z3"),
        F.count(F.when(F.col("zone") == "Z4", 1)).alias("n_zone_z4"),
        F.count("*").alias("n_rows"),
        F.countDistinct("zone").alias("distinct_zones"),
    )
)

annotated = (
    stats
    .select(
        "read_at_day",
        "site_id",
        "n_zone_z3",
        "n_zone_z4",
        "n_rows",
        "distinct_zones",
        F.abs(F.col("n_zone_z4") - 4).alias("n_zone_z4_distance"),
    )
)

result = (
    annotated
    .select(
        "read_at_day",
        "site_id",
        "n_zone_z4",
        "n_rows",
        "distinct_zones",
        "n_zone_z4_distance",
        F.greatest(F.col("distinct_zones"), F.col("n_zone_z3")).alias("greatest_distinct_zones_n_zone_z3"),
        F.date_format(F.col("read_at_day"), "yyyy-MM-dd").alias("read_at_day_label"),
    )
    .orderBy(
        F.col("site_id").desc_nulls_first(),
        F.col("distinct_zones").asc_nulls_last(),
        "read_at_day",
    )
    .limit(10)
)
