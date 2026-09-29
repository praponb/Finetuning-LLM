from pyspark.sql import functions as F

filtered_tariffs = (
    df_tariff_windows
    .filter((F.col("rate") > 2.2) & (F.col("valid_from") < F.lit("2025-03-09 04:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.concat(F.col("tariff"), F.lit("#"), F.col("id").cast("string")).alias("tariff_ref"),
    )
)

tariff_rows = filtered_tariffs.select(F.col("id").alias("ref_id"), "tariff_ref")

site_rows = df_sites.select("id", "tariff")

result = (
    tariff_rows
    .intersectAll(site_rows)
    .orderBy("ref_id", "tariff_ref")
)
