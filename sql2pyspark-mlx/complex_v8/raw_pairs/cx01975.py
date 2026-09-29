from pyspark.sql import functions as F

site_rows = (
    df_sites
    .select(
        "id",
        F.col("site_name").alias("label"),
        F.lit("site").alias("record_type"),
    )
)

reading_rows = (
    df_meter_readings
    .select(
        F.col("site_id").alias("id"),
        F.col("quality").alias("label"),
        F.lit("reading").alias("record_type"),
    )
)

site_side = (
    df_sites
    .filter((F.col("commissioned_on").between(F.lit("2024-08-24").cast("date"), F.lit("2025-05-10").cast("date"))) & (F.col("zone") == "Z4"))
    .select(
        "id",
        F.col("tariff").alias("label"),
        F.lit("site_3").alias("record_type"),
    )
)

result = (
    site_rows
    .unionByName(reading_rows)
    .unionByName(site_side)
    .orderBy("id", "label")
)
