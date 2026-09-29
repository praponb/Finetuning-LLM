from pyspark.sql import functions as F

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

rollup_base = (
    covered
    .groupBy("zone")
    .agg(
        F.max("read_at").alias("last_read_at"),
        F.sum(F.when(F.col("zone") != "Z4", F.col("voltage")).otherwise(0)).alias("voltage_zone_z4"),
        F.countDistinct("zone").alias("distinct_zones"),
    )
)

result = (
    rollup_base
    .groupingSets(
        [["distinct_zones", "zone"], ["distinct_zones"], ["zone"]],
        "distinct_zones", "zone",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum(F.when(F.col("zone").isNotNull(), 1).otherwise(0)).alias("n_zone_is_not_null"),
    )
    .orderBy(
        F.col("distinct_zones").asc_nulls_last(),
        F.col("zone").asc_nulls_last(),
    )
)
