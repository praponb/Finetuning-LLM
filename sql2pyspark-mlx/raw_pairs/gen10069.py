from pyspark.sql import functions as F

valid = (
    df_device_readings
    .filter(F.col("reading_ts") >= F.to_date(F.lit("2025-09-01")))
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.round((F.col("status_bits") * 1.07), 2).alias("status_bits_adjusted"),
    )
)

result = (
    valid
    .select("id", "status_bits_adjusted", F.col("temp_c").alias("TEMP_C"))
    .unpivot(
        ids=["id"],
        values=["status_bits_adjusted", "TEMP_C"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
)
