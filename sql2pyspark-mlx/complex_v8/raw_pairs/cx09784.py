from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter((F.col("voltage") >= 213.7) & (F.col("quality") == "BAD"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

joined = (
    filtered.alias("s")
    .join(df_sites.alias("si"), (F.col("si.id") == F.col("s.site_id")) & (F.col("si.tariff").isin("TOU", "FLAT")), "inner")
    .filter(F.col("si.zone") == "Z3")
    .select("s.id", "s.site_id", F.col("si.zone").alias("site_zone"))
)

result = joined.select("id", "site_id", "site_zone")
