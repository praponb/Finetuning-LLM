from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("pressure").desc_nulls_last(), "id")
w2 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc())
w3 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_first(), "id")
w4 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.partitionBy("device_code")
w6 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_part = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-900, Window.currentRow)
w8 = Window.orderBy(F.col("humidity").desc())
w9 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w10 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("device_code").orderBy("humidity_max_1h", "id")
w12 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w13 = Window.orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w14 = Window.partitionBy("device_code").orderBy("next_humidity_max_1h", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w15 = Window.partitionBy(F.date_trunc("WEEK", "reading_ts"), "device_code").orderBy("session_id_avg_2h", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w16 = Window.orderBy(F.col("humidity_max_1h").desc(), "id")

ranked = (
    df_device_readings
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

ranked_device_readings = (
    ranked
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

ranked_device_readings_2 = (
    ranked_device_readings
    .withColumn("rn_2", F.row_number().over(w3))
    .filter(F.col("rn_2") == 1)
    .select("id", "device_code", "reading_ts", "humidity", "firmware", "rn")
)

with_windows = (
    ranked_device_readings_2
    .select(
        "*",
        F.max("humidity").over(w4).alias("humidity_max_1h"),
        F.round((F.col("humidity") / F.sum("humidity").over(w5)), 4).alias("humidity_share"),
        F.avg("humidity").over(w6).alias("humidity_avg_1h"),
        F.avg("humidity").over(w_part).alias("humidity_avg_15m"),
        F.percent_rank().over(w8).alias("humidity_percent_rank"),
    )
)

flagged = (
    with_windows
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w9) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = (
    flagged
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity_max_1h",
        F.sum("new_group_flag").over(w10).alias("session_id"),
    )
)

enriched = (
    sessions_device_readings
    .withColumn("first_known_humidity_max_1h", F.first("humidity_max_1h", ignorenulls=True).over(w10))
    .withColumn("next_humidity_max_1h", F.lead("humidity_max_1h", 1).over(w11))
    .withColumn("session_id_avg_2h", F.avg("session_id").over(w12))
    .withColumn("humidity_max_1h_share", F.round((F.col("humidity_max_1h") / F.sum("humidity_max_1h").over(w5)), 4))
    .withColumn("humidity_max_1h_sum_30m", F.sum("humidity_max_1h").over(w13))
)

metrics = (
    enriched
    .filter(F.col("first_known_humidity_max_1h") >= 41)
    .select(
        "id",
        "humidity_max_1h_share",
        "humidity_max_1h_sum_30m",
        F.sum("humidity_max_1h").over(w14).alias("running_humidity_max_1h"),
        F.first("humidity_max_1h_share", ignorenulls=True).over(w15).alias("first_known_humidity_max_1h_share"),
        F.avg("humidity_max_1h").over(w12).alias("humidity_max_1h_avg_2h"),
        F.ntile(4).over(w16).alias("humidity_max_1h_bucket"),
    )
)

result = (
    metrics
    .select(
        "id",
        "humidity_max_1h_share",
        "humidity_max_1h_sum_30m",
        "running_humidity_max_1h",
    )
    .orderBy(F.col("id").desc_nulls_last(), "humidity_max_1h_sum_30m")
)
