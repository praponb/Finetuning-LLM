from pyspark.sql import functions as F

recent = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("read_at")).alias("days_since_read_at"),
    )
)

reading_rows = recent.select("id", F.col("quality").alias("name"))

site_rows = df_sites.select("id", "site_name")

result = (
    reading_rows
    .exceptAll(site_rows)
    .orderBy("name", "id")
)
