from pyspark.sql import functions as F

scoped_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.when(F.col("voltage") >= 239.6, "HIGH").when(F.col("voltage") >= 223.3, "MEDIUM").otherwise("LOW").alias("voltage_tier"),
    )
)

reading_side = (
    scoped_readings
    .filter(F.col("voltage") <= 233.8)
    .select(
        F.lit("reading").alias("origin"),
        "id",
        F.col("read_at").alias("happened_at"),
        "site_id",
    )
)

reading_side_2 = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        F.lit("reading_2").alias("origin"),
        "id",
        F.col("read_at").alias("happened_at"),
        "site_id",
    )
)

result = reading_side.unionByName(reading_side_2)
