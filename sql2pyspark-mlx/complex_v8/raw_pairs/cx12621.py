from pyspark.sql import functions as F

covered = (
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
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

zone_stats = (
    covered
    .groupBy("zone")
    .agg(F.avg("rate").alias("avg_rate"))
)

above_average = (
    covered.alias("s")
    .join(zone_stats.alias("g"), F.col("g.zone") == F.col("s.zone"), "inner")
    .filter(F.col("s.rate") > F.col("g.avg_rate"))
    .select(
        "s.*",
        "g.avg_rate",
        F.round(F.col("s.rate") - F.col("g.avg_rate"), 2).alias("rate_minus_avg"),
    )
)

result = (
    above_average
    .groupingSets(
        [["site_id", "zone", "quality"], ["site_id"], ["zone"], []],
        "site_id", "zone", "quality",
    )
    .agg(
        F.grouping("site_id").alias("is_total_site_id"),
        F.count(F.when(F.col("zone").isin("Z2", "Z3"), 1)).alias("n_zone_in_z2_z3"),
        F.min("read_at").alias("first_read_at"),
        F.count(F.when(F.col("quality").isin("BAD", "OK"), 1)).alias("n_quality_in_bad_ok"),
    )
    .select(
        "site_id",
        F.coalesce(F.col("zone"), F.lit("ALL")).alias("zone"),
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "is_total_site_id",
        "n_zone_in_z2_z3",
        "first_read_at",
        "n_quality_in_bad_ok",
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("zone").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
    )
)
