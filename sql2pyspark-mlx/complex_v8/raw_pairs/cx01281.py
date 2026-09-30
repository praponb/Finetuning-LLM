from pyspark.sql import functions as F

site_side = (
    df_sites
    .filter(F.col("commissioned_on") < F.lit("2024-11-21").cast("date"))
    .select(
        F.lit("site").alias("record_type"),
        F.col("id").alias("entity_id"),
        "tariff",
    )
)

tariff_side = df_tariff_windows.select(F.lit("tariff").alias("record_type"), "id", "tariff")

result = (
    site_side
    .union(tariff_side)
    .orderBy("record_type")
)
