from pyspark.sql import functions as F

long_format = (
    df_device_readings
    .select(
        "id",
        "device_code",
        F.col("temp_c").alias("TEMP_C_VAL"),
        F.col("pressure").alias("PRESSURE_VAL"),
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["TEMP_C_VAL", "PRESSURE_VAL"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
)

result = (
    long_format
    .groupBy("metric")
    .agg(
        F.sum("reading").alias("total_reading"),
        F.percentile_approx("reading", [0.25, 0.5, 0.75]).alias("reading_quartiles"),
        F.count(F.when(F.col("metric") == "TEMP_C_VAL", 1)).alias("temp_c_val_rows"),
        F.percentile_approx("reading", 0.9).alias("reading_p90"),
        F.avg(F.when(F.col("metric") == "TEMP_C_VAL", F.col("reading"))).alias("temp_c_val_avg_reading"),
    )
    .filter(F.col("temp_c_val_avg_reading") > 0)
)
