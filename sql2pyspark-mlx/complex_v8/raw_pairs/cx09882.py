from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("quality", "site_id").orderBy("read_at", "id")
w_site_id_quality = Window.partitionBy("site_id", "quality")
w_site_id_quality_by_voltage_t = Window.partitionBy("site_id", "quality").orderBy("voltage")

cleaned_readings = df_meter_readings.filter(F.col("kwh") < 106.5)

unique_rows = (
    cleaned_readings
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_activity = (
    unique_rows.alias("s")
    .join(df_meter_readings.alias("m"), (F.col("m.site_id") == F.col("s.site_id")) & (F.col("m.read_at") >= F.lit("2025-02-06 15:00:00").cast("timestamp")) & (F.col("m.voltage").between(213.5, 233.6)), "left_anti")
    .filter(F.col("kwh") >= 33.9)
)

metrics = (
    with_activity
    .select(
        "*",
        F.dense_rank().over(w_site_id_quality_by_voltage_t).alias("voltage_dense_rank"),
        F.round(F.avg("kwh").over(w_site_id_quality), 2).alias("site_id_quality_avg_kwh"),
    )
)

result = (
    metrics
    .cube("quality", "voltage_dense_rank", "site_id")
    .agg(
        F.max("read_at").alias("last_read_at"),
        F.max("site_id_quality_avg_kwh").alias("max_site_id_quality_avg_kwh"),
        F.round(100 * F.count(F.when(~F.col("quality").isin("EST", "BAD"), 1)) / F.count("*"), 1).alias("pct_quality_not_in_est_bad"),
    )
    .select(
        F.coalesce(F.col("quality"), F.lit("ALL")).alias("quality"),
        "voltage_dense_rank",
        "site_id",
        "last_read_at",
        "max_site_id_quality_avg_kwh",
        "pct_quality_not_in_est_bad",
    )
)
