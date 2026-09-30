from pyspark.sql import functions as F

unpivoted = (
    df_device_readings
    .select(
        "id",
        "device_code",
        F.col("humidity").alias("HUMIDITY_VAL"),
        F.col("pressure").alias("PRESSURE_VAL"),
        F.col("temp_c").alias("TEMP_C_VAL"),
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["HUMIDITY_VAL", "PRESSURE_VAL", "TEMP_C_VAL"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)

result = (
    unpivoted
    .rollup(
        F.when(F.col("metric_value") >= 75, "LARGE").otherwise("SMALL").alias("metric_value_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("measure")).alias("measure_list"),
        F.count_if(F.col("device_code") == "DV-02").alias("dv_02_events"),
        F.count_if(F.col("measure") == "TEMP_C_VAL").alias("temp_c_val_events"),
    )
    .orderBy("dv_02_events")
)
