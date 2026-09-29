from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy("pressure", "id")
w3 = Window.partitionBy("device_code").orderBy("running_min_pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_device_readings
    .filter(F.col("device_code").isin(["DV-03", "DV-01"]))
    .select(
        "id",
        "device_code",
        "pressure",
        "firmware",
        F.round((F.col("pressure") * 0.85), 2).alias("adjusted_pressure"),
    )
)

filtered = (
    base
    .filter(F.col("pressure").between(996, 1005))
    .select(
        "id",
        "device_code",
        "pressure",
        "firmware",
        "adjusted_pressure",
        F.when(F.col("firmware").isin(["1.1.0", "2.0.1"]), "Core").otherwise("Other").alias("firmware_group"),
    )
)

scored = (
    filtered
    .filter(~F.col("firmware").isin(["2.0.1", "1.1.0"]))
    .select(
        "*",
        F.count("pressure").over(w_running).alias("running_count_pressure"),
        F.sum("pressure").over(w_running).alias("running_sum_pressure"),
        F.min("pressure").over(w_running).alias("running_min_pressure"),
        F.max("pressure").over(w_running).alias("running_max_pressure"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

metrics = (
    scored
    .select(
        "*",
        F.max("running_count_pressure").over(w3).alias("running_max_running_count_pressure"),
        F.min("running_count_pressure").over(w3).alias("running_min_running_count_pressure"),
        F.sum("running_count_pressure").over(w3).alias("running_sum_running_count_pressure"),
        F.sum("pressure").over(w4).alias("cumulative_pressure"),
    )
)

base_device_readings = (
    metrics
    .filter(F.col("running_min_pressure").between(1004, 1011))
    .withColumn("running_max_running_count_pressure_tier", F.when(F.col("running_max_running_count_pressure") >= 14.5, "High").when(F.col("running_max_running_count_pressure") >= 4.8, "Medium").otherwise("Low"))
)

result = (
    base_device_readings
    .groupBy(
        F.when(F.col("row_seq") >= 9, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
        F.when(F.col("cumulative_pressure") >= 4665, "LARGE").otherwise("SMALL").alias("cumulative_pressure_tier"),
    )
    .agg(
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
        F.round(F.sum("adjusted_pressure") / F.nullif(F.count(F.when(F.col("device_code") == "DV-03", 1)), F.lit(0)), 2).alias("adjusted_pressure_per_dv_03"),
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_rows"),
        F.countDistinct("firmware").alias("distinct_firmwares"),
    )
)
