from pyspark.sql import functions as F

base = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        F.round((F.col("status_bits") / F.col("humidity")), 2).alias("status_bits_per_humidity"),
    )
)

result = (
    base
    .select("id", "status_bits_per_humidity", "pressure", "humidity")
    .unpivot(
        ids=["id"],
        values=["status_bits_per_humidity", "pressure", "humidity"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
