from pyspark.sql import functions as F

result = (
    df_device_readings
    .groupBy("firmware")
    .pivot("device_code", ["DV-02", "DV-01", "DV-03"])
    .agg(F.max("humidity").alias("total"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "firmware",
        F.col("DV-02_total").alias("dv_02_total"),
        F.col("DV-02_item_count").alias("dv_02_item_count"),
        F.col("DV-01_total").alias("dv_01_total"),
        F.col("DV-01_item_count").alias("dv_01_item_count"),
        F.col("DV-03_total").alias("dv_03_total"),
        F.col("DV-03_item_count").alias("dv_03_item_count"),
    )
)
