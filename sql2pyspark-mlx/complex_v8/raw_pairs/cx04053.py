from pyspark.sql import functions as F

filtered = (
    df_sites
    .filter((F.col("installed_kw") < 373.3) & (F.col("commissioned_on").between(F.lit("2024-06-27").cast("date"), F.lit("2025-05-10").cast("date"))))
    .select(
        "id",
        "tariff",
        "installed_kw",
        "commissioned_on",
        F.date_format(F.col("commissioned_on"), "yyyy-MM-dd").alias("commissioned_on_label"),
    )
)

screened = (
    filtered.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.id")) & (F.col("m.quality") != "EST"), "left_semi")
)

scored = (
    screened
    .select(
        "id",
        "tariff",
        "installed_kw",
        "commissioned_on",
        "commissioned_on_label",
        F.concat_ws("|", F.col("commissioned_on_label"), F.col("tariff")).alias("commissioned_on_label_tariff_label"),
    )
)

result = (
    scored
    .groupBy("commissioned_on")
    .pivot("tariff", ["EV", "TOU"])
    .agg(F.avg("installed_kw"))
)
