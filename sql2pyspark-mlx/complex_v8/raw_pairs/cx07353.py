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
        "r.quality",
        "st.zone",
        "tw.rate",
        F.round(F.col("r.kwh") * F.col("tw.rate"), 2).alias("energy_cost"),
    )
)

scored = (
    matched
    .filter(F.col("kwh") > 16.5)
    .select(
        "*",
        F.abs(F.col("kwh") - 43.1).alias("kwh_distance"),
        F.coalesce(F.col("kwh"), F.lit(0.0)).alias("kwh_filled"),
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
    )
)

result = (
    scored
    .select(
        "id",
        "site_id",
        "read_at",
        "quality",
        "zone",
        "rate",
        "kwh_distance",
        "kwh_filled",
        "read_at_label",
        (F.floor(F.col("kwh_filled") / 25) * 25).alias("kwh_filled_bucket"),
        F.coalesce(F.col("zone"), F.lit("UNKNOWN")).alias("zone_or_unknown"),
    )
    .orderBy(
        F.col("read_at_label").desc_nulls_first(),
        F.col("read_at").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
