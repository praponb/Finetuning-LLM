from pyspark.sql import functions as F

prepared = (
    df_meter_readings
    .filter((F.col("voltage") < 232.7) & (F.hour(F.col("read_at")).between(9, 11)))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

reading_side = (
    prepared
    .filter((F.col("read_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp")) & (F.col("quality") == "OK"))
    .select(
        F.lit("reading").alias("origin"),
        F.col("id").alias("entity_id"),
        "site_id",
        "read_at",
    )
)

reading_rows = (
    df_meter_readings
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-03-08").cast("date"))
    .select(
        F.lit("reading_2").alias("origin"),
        F.col("id").alias("entity_id"),
        "site_id",
        "read_at",
    )
)

result = (
    reading_side
    .unionByName(reading_rows)
    .distinct()
    .orderBy(F.col("read_at").desc())
)
