from pyspark.sql import functions as F

filtered = (
    df_meter_readings
    .filter(F.col("kwh") <= 79.4)
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

reading_rows = (
    filtered
    .filter(F.col("site_id").isNotNull())
    .select("site_id", "id", F.lit("reading").alias("origin"))
)

reading_side = df_meter_readings.select("site_id", "id", F.lit("reading_2").alias("origin"))

reading_rows_2 = (
    df_meter_readings
    .filter(F.col("kwh") < 111.9)
    .select("id", "site_id", F.lit("reading_3").alias("origin"))
)

combined = (
    reading_rows
    .union(reading_side)
    .union(reading_rows_2)
)

result = (
    combined
    .groupBy("origin")
    .agg(F.count("*").alias("n_rows"), F.sum("site_id").alias("total_site_id"))
    .orderBy("origin")
)
