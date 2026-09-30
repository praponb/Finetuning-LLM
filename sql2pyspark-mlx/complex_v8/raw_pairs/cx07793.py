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

grouped = (
    covered
    .groupBy("zone")
    .agg(
        F.sum("kwh").alias("total_kwh"),
        F.countDistinct("zone").alias("distinct_zones"),
    )
    .filter(F.col("total_kwh") > 42.1)
)

labeled = (
    grouped
    .select(
        "*",
        F.concat(F.col("zone"), F.lit("#"), F.col("distinct_zones").cast("string")).alias("zone_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("zone")), " ", "_")).alias("zone_slug"),
    )
)

result = (
    labeled
    .select(
        "zone",
        "total_kwh",
        "distinct_zones",
        "zone_ref",
        "zone_slug",
        F.coalesce(F.col("distinct_zones"), F.lit(0)).alias("distinct_zones_filled"),
    )
    .orderBy(F.col("zone").desc(), F.col("total_kwh").desc())
)
