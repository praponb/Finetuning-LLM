from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code")
w_grp = Window.partitionBy("device_code").orderBy(F.col("humidity_share").desc())
w3 = Window.partitionBy("device_code").orderBy(F.col("status_bits_rounded").desc_nulls_last())
w4 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w_running = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_first())
w6 = Window.orderBy(F.col("humidity_share").asc_nulls_last())
w7 = Window.orderBy(F.col("humidity").asc_nulls_last())

base = (
    df_device_readings
    .filter(F.col("device_code") == "DV-02")
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        F.when(F.col("pressure") >= 1032, "High").when(F.col("pressure") >= 987, "Medium").otherwise("Low").alias("pressure_tier"),
        F.round("status_bits", 2).alias("status_bits_rounded"),
    )
)

remaining_keys = (
    base
    .select("device_code", "id")
    .exceptAll(
        df_device_readings
        .filter(F.col("humidity") > 64)
        .select("device_code", "id")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "*",
        F.round((F.col("humidity") / F.sum("humidity").over(w)), 4).alias("humidity_share"),
    )
)

scored_device_readings = (
    scored
    .select(
        "reading_ts",
        "humidity",
        "status_bits_rounded",
        "humidity_share",
        F.dense_rank().over(w_grp).alias("humidity_share_dense_rank"),
        F.rank().over(w3).alias("status_bits_rounded_rank"),
        F.count("humidity").over(w4).alias("humidity_count_1h"),
        F.dense_rank().over(w_running).alias("humidity_dense_rank"),
    )
)

scored_device_readings_2 = (
    scored_device_readings
    .filter(F.col("humidity") >= 42)
    .select(
        "reading_ts",
        "status_bits_rounded",
        "humidity_share_dense_rank",
        "humidity_count_1h",
        "humidity_dense_rank",
        F.dense_rank().over(w6).alias("humidity_share_dense_rank_2"),
        F.dense_rank().over(w7).alias("humidity_dense_rank_2"),
    )
)

result = (
    scored_device_readings_2
    .select(
        "reading_ts",
        "status_bits_rounded",
        "humidity_share_dense_rank",
        "humidity_count_1h",
        "humidity_dense_rank",
        "humidity_share_dense_rank_2",
        "humidity_dense_rank_2",
    )
    .orderBy("humidity_share_dense_rank_2")
)
