from pyspark.sql import functions as F

long_format = (
    df_device_readings
    .select("id", "device_code", "temp_c", "humidity", "pressure")
    .unpivot(
        ids=["id", "device_code"],
        values=["temp_c", "humidity", "pressure"],
        variableColumnName="measure",
        valueColumnName="value",
    )
)

result = (
    long_format
    .groupBy(
        F.when(F.col("value") >= 76, "LARGE").otherwise("SMALL").alias("value_tier"),
    )
    .agg(
        F.countDistinct("device_code").alias("distinct_device_codes"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_rows"),
        F.size(F.collect_set("measure")).alias("num_measures"),
    )
    .orderBy(F.col("distinct_device_codes").desc())
)
