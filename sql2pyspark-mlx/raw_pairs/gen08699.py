from pyspark.sql import functions as F

unpivoted = (
    df_device_readings
    .select("id", "device_code", "humidity", "temp_c", "pressure")
    .unpivot(
        ids=["id", "device_code"],
        values=["humidity", "temp_c", "pressure"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)

result = (
    unpivoted
    .withColumn("val_tier", F.when(F.col("val") >= 71, "PREMIUM").otherwise("STANDARD"))
    .rollup("measure", "val_tier", "device_code")
    .agg(
        F.percentile_approx("val", 0.95).alias("val_p95"),
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
        F.sum("val").alias("total_val"),
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles_2"),
        F.count("*").alias("cnt"),
        F.grouping("measure").alias("gid"),
    )
    .orderBy(F.col("device_code").desc_nulls_last())
)
