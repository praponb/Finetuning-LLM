from pyspark.sql import functions as F

cleaned_readings = (
    df_meter_readings
    .filter((~F.col("quality").isin("OK", "EST")) & (F.col("kwh").between(37.1, 81.9)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.lower(F.regexp_replace(F.trim(F.col("quality")), " ", "_")).alias("quality_slug"),
    )
)

reading_rows = (
    cleaned_readings
    .filter(F.col("site_id") <= 5)
    .select("id")
)

reading_side = (
    df_meter_readings
    .filter(F.col("quality") != "BAD")
    .select("id")
)

result = (
    reading_rows
    .exceptAll(reading_side)
    .orderBy(F.col("id").desc())
)
