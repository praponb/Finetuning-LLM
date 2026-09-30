from pyspark.sql import functions as F

prepared_tariffs = (
    df_tariff_windows
    .filter((F.col("tariff").isNotNull()) | (F.col("valid_to") >= F.lit("2025-01-25 21:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
    )
)

tariff_side = prepared_tariffs.select("id", F.col("tariff").alias("name"))

site_rows = (
    df_sites
    .filter((F.col("tariff") != "TOU") & (F.col("commissioned_on").between(F.lit("2024-06-27").cast("date"), F.lit("2025-05-10").cast("date"))))
    .select("id", F.col("zone").alias("name"))
)

result = (
    tariff_side
    .subtract(site_rows)
    .orderBy("name")
)
