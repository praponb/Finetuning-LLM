from pyspark.sql import functions as F

valid = (
    df_sites
    .filter(F.upper(F.trim(F.col("site_name"))).like("H%"))
    .select(
        "id",
        "zone",
        "tariff",
        "installed_kw",
        "commissioned_on",
    )
)

site_side = (
    valid
    .filter((F.col("commissioned_on") < F.lit("2024-11-21").cast("date")) & (F.col("tariff") != "FLAT"))
    .select(F.col("id").alias("entity_id"))
)

tariff_rows = df_tariff_windows.select(F.col("id").alias("entity_id"))

result = (
    site_side
    .subtract(tariff_rows)
    .orderBy("entity_id")
)
