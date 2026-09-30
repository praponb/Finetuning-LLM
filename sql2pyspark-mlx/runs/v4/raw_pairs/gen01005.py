from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy(F.col("pressure_share").desc_nulls_first(), "id")

cleaned = (
    df_device_readings
    .filter(F.col("humidity").between(59, 70))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        F.to_date("reading_ts").alias("reading_ts_date"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.round((F.col("pressure") / F.sum("pressure").over(w)), 4).alias("pressure_share"),
        F.first("pressure", ignorenulls=True).over(w2).alias("first_known_pressure"),
        F.round((F.col("temp_c") / F.sum("temp_c").over(w)), 4).alias("temp_c_share"),
        F.first("pressure", ignorenulls=True).over(w3).alias("first_known_pressure_2"),
    )
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("HOUR", "reading_ts").alias("hour_window"))
    .agg(
        F.round(F.avg("pressure_share"), 2).alias("avg_pressure_share"),
        F.sort_array(F.collect_set("device_code")).alias("device_code_list"),
        F.count_if(F.col("device_code") == "DV-01").alias("dv_01_events"),
    )
)
