from pyspark.sql import functions as F

scoped_tariffs = (
    df_tariff_windows
    .filter(F.col("tariff") != "TOU")
    .select(
        "id",
        "tariff",
        "valid_to",
        "rate",
    )
)

tariff_side = (
    scoped_tariffs
    .filter((F.col("tariff").isin("FLAT", "EV")) & (F.col("rate").between(3.0, 7.8)))
    .select(F.col("id").alias("qty"))
)

site_rows = (
    df_sites
    .filter(F.col("commissioned_on").between(F.lit("2024-08-24").cast("date"), F.lit("2025-11-28").cast("date")))
    .select(F.col("id").alias("qty"))
)

result = (
    tariff_side
    .intersect(site_rows)
    .orderBy("qty")
)
