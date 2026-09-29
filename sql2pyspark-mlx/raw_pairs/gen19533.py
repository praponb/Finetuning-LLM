from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w2 = Window.orderBy("is_1_0_3", "id")
w3 = Window.partitionBy("device_code").orderBy("device_code_sum_is_1_1_0", "id")
w4 = Window.partitionBy("device_code").orderBy("prev_is_1_1_0", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("device_code").orderBy("prev_is_1_1_0", "id")
w6 = Window.partitionBy("device_code").orderBy("prev_prev_is_1_1_0", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("device_code").orderBy("seq_no_2", "id")
w8 = Window.orderBy("device_code_sum_is_1_1_0", "id")
w9 = Window.orderBy("prev_prev_is_1_1_0", "id")

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") > F.to_date(F.lit("2025-09-01"))) & (F.col("status_bits") < 11))
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "pressure",
        "status_bits",
        "firmware",
        F.when(F.col("firmware") == "1.0.3", 1).otherwise(0).alias("is_1_0_3"),
        F.when(F.col("firmware") == "1.1.0", 1).otherwise(0).alias("is_1_1_0"),
    )
)

with_windows = (
    filtered
    .filter(F.col("temp_c").between(31, 35))
    .select(
        "id",
        "device_code",
        "temp_c",
        "pressure",
        "firmware",
        "is_1_0_3",
        "is_1_1_0",
        F.sum("is_1_1_0").over(w).alias("device_code_sum_is_1_1_0"),
        F.lag("is_1_1_0", 1).over(w2).alias("prev_is_1_1_0"),
    )
)

enriched = (
    with_windows
    .select(
        "*",
        F.lead("prev_is_1_1_0").over(w3).alias("next_prev_is_1_1_0"),
        F.lag("prev_is_1_1_0").over(w3).alias("prev_prev_is_1_1_0"),
        F.lead("is_1_0_3").over(w3).alias("next_is_1_0_3"),
        F.lag("is_1_0_3").over(w3).alias("prev_is_1_0_3"),
        F.row_number().over(w3).alias("seq_no"),
        F.last("pressure", ignorenulls=True).over(w4).alias("last_known_pressure"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "device_code",
        "pressure",
        "firmware",
        "is_1_1_0",
        "device_code_sum_is_1_1_0",
        "prev_prev_is_1_1_0",
        "next_is_1_0_3",
        "prev_is_1_0_3",
        "seq_no",
        "last_known_pressure",
        F.lead("next_prev_is_1_1_0").over(w5).alias("next_next_prev_is_1_1_0"),
        F.lag("next_is_1_0_3").over(w5).alias("prev_next_is_1_0_3"),
        F.lead("next_is_1_0_3").over(w5).alias("next_next_is_1_0_3"),
        F.row_number().over(w5).alias("seq_no_2"),
        F.avg("next_is_1_0_3").over(w6).alias("next_is_1_0_3_avg_5"),
    )
)

with_windows_device_readings = (
    metrics
    .withColumn("event_seq", F.row_number().over(w7))
    .withColumn("prev_next_next_is_1_0_3", F.lag("next_next_is_1_0_3", 1).over(w8))
    .withColumn("seq_no_2_share", F.round((F.col("seq_no_2") / F.sum("seq_no_2").over(w)), 4))
    .withColumn("prev_prev_is_1_1_0_bucket", F.ntile(4).over(w9))
)

daily = (
    with_windows_device_readings
    .groupBy("seq_no")
    .agg(
        F.count(F.when(F.col("device_code") == "DV-03", 1)).alias("dv_03_count"),
        F.max("prev_prev_is_1_1_0_bucket").alias("max_prev_prev_is_1_1_0_bucket"),
        F.round(F.sum("prev_prev_is_1_1_0") / F.nullif(F.count(F.when(F.col("device_code") == "DV-01", 1)), F.lit(0)), 2).alias("prev_prev_is_1_1_0_per_dv_01"),
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count"),
    )
    .filter(F.col("max_prev_prev_is_1_1_0_bucket") > 0)
)

result = (
    daily
    .select(
        "seq_no",
        "dv_03_count",
        "max_prev_prev_is_1_1_0_bucket",
        "prev_prev_is_1_1_0_per_dv_01",
        "dv_02_count",
    )
    .orderBy(F.col("prev_prev_is_1_1_0_per_dv_01").desc())
)
