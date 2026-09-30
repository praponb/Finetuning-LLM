from pyspark.sql import functions as F

unpivoted = (
    df_device_readings
    .select(
        "id",
        "device_code",
        F.col("pressure").alias("PRESSURE_VAL"),
        F.col("temp_c").alias("TEMP_C"),
    )
    .unpivot(
        ids=["id", "device_code"],
        values=["PRESSURE_VAL", "TEMP_C"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)

result = (
    unpivoted
    .cube(F.when(F.col("val") >= 71, "PREMIUM").otherwise("STANDARD").alias("val_tier"))
    .agg(
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles_2"),
        F.count(F.when(F.col("device_code") == "DV-01", 1)).alias("dv_01_count"),
    )
)
