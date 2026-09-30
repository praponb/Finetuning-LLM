from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy(F.date_trunc("MONTH", "reading_ts"), "device_code").orderBy("pressure", "id")
w3 = Window.partitionBy(F.to_date("reading_ts"), "device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.to_date("reading_ts")).orderBy("running_pressure_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("running_pressure_int", "id")

valid = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .withColumn("device_code_firmware", F.concat("device_code", F.lit(", "), "firmware"))
)

base = (
    valid
    .filter(F.col("reading_ts") > F.lit("2025-09-01 06:00:00").cast("timestamp"))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.col("pressure").cast("int").alias("pressure_int"),
        F.when(F.col("humidity") >= 65, "High").when(F.col("humidity") >= 45, "Medium").otherwise("Low").alias("humidity_tier"),
    )
)

enriched = (
    base
    .select(
        "id",
        "reading_ts",
        "humidity",
        "pressure",
        "firmware",
        "pressure_int",
        "humidity_tier",
        F.corr("status_bits", "pressure").over(w).alias("corr_status_bits_pressure"),
        F.row_number().over(w2).alias("event_seq"),
        F.sum("pressure_int").over(w3).alias("running_pressure_int"),
    )
)

enriched_device_readings = (
    enriched
    .select(
        "*",
        F.sum("running_pressure_int").over(w4).alias("running_running_pressure_int"),
        F.ntile(5).over(w5).alias("running_pressure_int_bucket"),
    )
)

result = (
    enriched_device_readings
    .select("id", "corr_status_bits_pressure", "humidity", "pressure")
    .unpivot(
        ids=["id"],
        values=["corr_status_bits_pressure", "humidity", "pressure"],
        variableColumnName="kpi",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
