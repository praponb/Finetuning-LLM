from pyspark.sql import functions as F

cleaned = (
    df_device_readings
    .filter(~F.col("device_code").isin(["DV-04", "DV-03"]))
    .select(
        "id",
        "device_code",
        "humidity",
        "firmware",
        F.to_date("reading_ts").alias("reading_ts_date"),
    )
)

result = (
    cleaned
    .groupBy("firmware")
    .pivot("device_code", ["DV-03", "DV-04"])
    .agg(F.avg("humidity").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "firmware",
        F.col("DV-03_value_sum").alias("dv_03_value_sum"),
        F.col("DV-03_item_count").alias("dv_03_item_count"),
        F.col("DV-04_value_sum").alias("dv_04_value_sum"),
        F.col("DV-04_item_count").alias("dv_04_item_count"),
    )
)
