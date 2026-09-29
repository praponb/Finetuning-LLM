from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc())
w4 = Window.partitionBy("device_code").orderBy("row_num", "id")
w5 = Window.partitionBy("device_code").orderBy("session_id", "id").rowsBetween(-2, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("is_new_burst", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 15 MINUTES")), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure",
        "firmware",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

latest = (
    islands
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

deduped = (
    latest
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    deduped
    .filter(~F.col("firmware").isin(["1.0.3", "2.0.1"]))
    .select(
        "id",
        "row_num",
        F.count("pressure").over(w5).alias("last3_count_pressure"),
        F.max("pressure").over(w5).alias("last3_max_pressure"),
        F.avg("pressure").over(w5).alias("last3_avg_pressure"),
        F.sum("pressure").over(w5).alias("last3_sum_pressure"),
    )
)

result = (
    metrics
    .select(
        "id",
        "row_num",
        "last3_count_pressure",
        "last3_max_pressure",
        "last3_avg_pressure",
        "last3_sum_pressure",
    )
    .orderBy(F.col("row_num").asc_nulls_last())
)
