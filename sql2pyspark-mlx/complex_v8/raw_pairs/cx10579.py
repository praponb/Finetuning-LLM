from pyspark.sql import functions as F

joined_ranges = (
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

joined_site = (
    joined_ranges.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select("s.*", F.col("si.zone").alias("site_zone"))
)

result = (
    joined_site
    .filter(F.col("rate") >= 2.2)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "energy_cost",
        "site_zone",
        F.date_format(F.col("read_at"), "MMM yyyy").alias("read_at_label"),
    )
    .orderBy(F.col("quality").desc(), F.col("read_at").desc_nulls_first())
)
