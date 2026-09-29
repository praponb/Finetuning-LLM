from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality_by_kwh_ties = Window.partitionBy("quality").orderBy(F.col("kwh").asc_nulls_last())
w_quality_by_kwh_moving_6 = Window.partitionBy("quality").orderBy(F.col("kwh").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_quality_by_kwh = Window.partitionBy("quality").orderBy(F.col("kwh").asc_nulls_last(), "id")
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_all_by_kwh_running = Window.orderBy("kwh", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_readings = (
    df_meter_readings
    .filter((F.col("read_at") >= F.lit("2025-01-19 07:00:00").cast("timestamp")) & (F.col("site_id").isNotNull()))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_windows = (
    cleaned_readings
    .select(
        "*",
        F.round(F.cume_dist().over(w_quality_by_kwh_ties), 3).alias("kwh_cume_dist"),
        F.round(F.avg("voltage").over(w_quality_by_kwh_moving_6), 2).alias("voltage_avg_7"),
        F.ntile(3).over(w_quality_by_kwh).alias("kwh_tercile"),
        F.row_number().over(w_quality_by_kwh).alias("seq_no"),
        F.sum("kwh").over(w_all_by_kwh_running).alias("running_kwh"),
    )
)

result = (
    with_windows
    .cube("kwh_tercile", "quality", "seq_no")
    .agg(
        F.grouping("kwh_tercile").alias("is_total_kwh_tercile"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("kwh_tercile").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
        F.col("seq_no").asc_nulls_last(),
    )
)
