from pyspark.sql import functions as F

base_readings = (
    df_meter_readings
    .filter((F.col("kwh") > 46.6) | (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

reading_side = (
    base_readings
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-03-08").cast("date"))
    .select(
        "id",
        "quality",
        "quality_len",
        F.lit("reading").alias("origin"),
    )
)

reading_side_2 = (
    df_meter_readings
    .select(
        "id",
        "quality",
        F.col("site_id").alias("quality_len"),
        F.lit("reading_2").alias("origin"),
    )
)

result = (
    reading_side
    .unionByName(reading_side_2)
    .distinct()
    .orderBy(F.col("origin").desc(), "quality")
)
