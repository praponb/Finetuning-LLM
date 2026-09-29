from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.orderBy(F.col("pressure").asc_nulls_first())
w3 = Window.partitionBy("device_code").orderBy("pressure", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w5 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w6 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy(F.col("prev_reading_ts").asc_nulls_first(), "id")

with_windows = (
    df_device_readings
    .withColumn("pressure_share", F.round((F.col("pressure") / F.sum("pressure").over(w)), 4))
    .withColumn("pressure_rank", F.rank().over(w2))
    .withColumn("cumulative_humidity", F.sum("humidity").over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("pressure_avg_2h", F.avg("pressure").over(w5))
)

gaps = (
    with_windows
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w4) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("session_id", F.sum("new_group_flag").over(w6))

with_prev = sessions_device_readings.withColumn("prev_reading_ts", F.lag("reading_ts").over(w4))

gaps_2 = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

islands = gaps_2.withColumn("session_no", F.sum("session_start_flag").over(w6))

ranked = (
    islands
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

by_type = (
    ranked
    .groupBy("firmware")
    .pivot("device_code", ["DV-02", "DV-03", "DV-01"])
    .agg(F.avg("pressure_avg_2h").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "firmware",
        F.col("DV-02_amount").alias("dv_02_amount"),
        F.col("DV-02_cnt").alias("dv_02_cnt"),
        F.col("DV-03_amount").alias("dv_03_amount"),
        F.col("DV-03_cnt").alias("dv_03_cnt"),
        F.col("DV-01_amount").alias("dv_01_amount"),
        F.col("DV-01_cnt").alias("dv_01_cnt"),
    )
)

result = (
    by_type
    .select(
        "firmware",
        "dv_02_amount",
        "dv_02_cnt",
        "dv_03_cnt",
        "dv_01_amount",
        "dv_01_cnt",
    )
    .orderBy("dv_01_amount")
)
