from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.lit("2025-09-01 05:00:00").cast("timestamp"))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.floor("humidity").alias("humidity_whole"),
    )
)

result = (
    valid
    .select(
        "id",
        F.col("pressure").alias("PRESSURE"),
        F.col("humidity").alias("HUMIDITY_VAL"),
    )
    .unpivot(
        ids=["id"],
        values=["PRESSURE", "HUMIDITY_VAL"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
