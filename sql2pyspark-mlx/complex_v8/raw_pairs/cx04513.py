from pyspark.sql import functions as F

scoped = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
    )
)

result = (
    scoped
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
    )
    .orderBy("kwh", F.col("voltage").desc())
)
