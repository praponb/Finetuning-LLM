from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(F.col("firmware") != "1.1.0")
    .select(
        "*",
        (F.col("status_bits") + F.col("humidity")).alias("status_bits_plus_humidity"),
        F.round((F.col("temp_c") / F.col("pressure")), 2).alias("temp_c_per_pressure"),
    )
)

result = (
    cleaned
    .select("id", "device_code", "pressure", "temp_c")
    .unpivot(
        ids=["id", "device_code"],
        values=["pressure", "temp_c"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
)
