from pyspark.sql import functions as F

reading_rows = (
    df_meter_readings
    .filter(F.col("read_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp"))
    .select(
        F.col("site_id").alias("entity_id"),
        F.col("quality").alias("label"),
        "id",
        F.lit("reading").alias("src"),
    )
)

reading_rows_2 = (
    df_meter_readings
    .select(
        "site_id",
        "quality",
        "id",
        F.lit("reading_2").alias("src"),
    )
)

result = (
    reading_rows
    .union(reading_rows_2)
    .distinct()
    .orderBy("label", "src")
)
