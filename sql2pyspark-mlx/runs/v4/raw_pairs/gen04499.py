from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)

filtered = (
    df_device_readings
    .filter(F.col("temp_c") <= 34)
    .withColumn("firmware_title", F.initcap(F.trim("firmware")))
)

enriched = (
    filtered
    .withColumn("first_known_temp_c", F.first("temp_c", ignorenulls=True).over(w))
    .withColumn("status_bits_sum_30m", F.sum("status_bits").over(w2))
)

base = (
    enriched
    .filter(F.to_date("reading_ts") >= "2025-01-01")
    .select(
        "id",
        "device_code",
        "temp_c",
        "status_bits",
        "firmware",
        "firmware_title",
        "status_bits_sum_30m",
        F.datediff(F.current_date(), "reading_ts").alias("days_since_reading_ts"),
        F.month("reading_ts").alias("reading_ts_month"),
    )
)

result = (
    base
    .rollup(
        F.when(F.col("temp_c") >= 34.5, "LARGE").otherwise("SMALL").alias("temp_c_tier"),
        "device_code",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("device_code") == "DV-04", 1)).alias("dv_04_rows"),
        F.round(F.avg("temp_c"), 2).alias("avg_temp_c"),
        F.avg(F.when(F.col("device_code") == "DV-04", F.col("temp_c"))).alias("dv_04_avg_temp_c"),
    )
    .orderBy(F.col("device_code").desc_nulls_first())
)
