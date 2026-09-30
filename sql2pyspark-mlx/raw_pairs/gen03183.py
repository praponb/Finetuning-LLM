from pyspark.sql import functions as F

filtered = (
    df_device_readings
    .filter(F.col("temp_c") > 24)
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        F.round("humidity", 2).alias("humidity_rounded"),
    )
)

result = (
    filtered
    .select("id", "pressure", F.col("temp_c").alias("TEMP_C_VAL"), "humidity_rounded")
    .unpivot(
        ids=["id"],
        values=["pressure", "TEMP_C_VAL", "humidity_rounded"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
