from pyspark.sql import functions as F

tariff_side = (
    df_tariff_windows
    .select(
        "id",
        "rate",
        "tariff",
        F.lit("tariff").alias("source"),
    )
)

site_side = (
    df_sites
    .filter((F.col("zone") != "Z3") & (F.col("commissioned_on").between(F.lit("2024-06-27").cast("date"), F.lit("2025-05-10").cast("date"))))
    .select(
        "id",
        "installed_kw",
        "site_name",
        F.lit("site").alias("source"),
    )
)

result = tariff_side.union(site_side)
