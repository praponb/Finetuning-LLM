from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy(F.col("reading_ts").desc(), "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w3 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.date_trunc("YEAR", "reading_ts"))
w5 = Window.partitionBy("device_code")
w6 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w7 = Window.orderBy(F.col("pressure_plus_humidity_max_1h").asc_nulls_last())
w8 = Window.orderBy(F.col("device_code_min_temp_c").desc_nulls_first())
w9 = Window.orderBy("pressure")
w10 = Window.orderBy(F.col("humidity_share").desc_nulls_first())

cleaned = (
    df_device_readings
    .filter(F.col("reading_ts") > F.lit("2025-09-01").cast("date"))
    .select(
        "*",
        F.concat("device_code", F.lit(" "), "firmware").alias("device_code_firmware"),
    )
)

ranked = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

base = (
    ranked
    .filter(~F.col("device_code").isin(["DV-03", "DV-02", "DV-01"]))
    .select(
        "*",
        (F.col("pressure") + F.col("status_bits")).alias("pressure_plus_status_bits"),
        (F.col("pressure") + F.col("humidity")).alias("pressure_plus_humidity"),
    )
)

with_gaps = (
    base
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w2) > F.expr("INTERVAL 20 MINUTES"), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("island_id", F.sum("session_start_flag").over(w3))

remaining_keys = (
    sessionized
    .select("device_code", "id")
    .intersectAll(
        df_device_readings
        .filter(~F.col("firmware").isin(["1.1.0", "2.0.1"]))
        .select("device_code", "id")
    )
)

verified = (
    sessionized
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.device_code") == F.col("k.device_code")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .filter(F.col("humidity") > 65)
    .select(
        "id",
        "humidity",
        "pressure",
        "device_code_firmware",
        "pressure_plus_humidity",
        "session_start_flag",
        "island_id",
        F.round((F.col("humidity") / F.sum("humidity").over(w4)), 4).alias("humidity_share"),
        F.min("temp_c").over(w5).alias("device_code_min_temp_c"),
        F.max("humidity").over(w6).alias("humidity_max_1h"),
    )
)

valid = (
    enriched
    .filter(F.col("session_start_flag") == 0)
    .withColumn("pressure_plus_humidity_max_1h", (F.col("pressure") + F.col("humidity_max_1h")))
)

result = (
    valid
    .select(
        "id",
        "device_code_firmware",
        "pressure_plus_humidity",
        "humidity_share",
        "device_code_min_temp_c",
        "humidity_max_1h",
        F.percent_rank().over(w7).alias("pressure_plus_humidity_max_1h_percent_rank"),
        F.dense_rank().over(w8).alias("device_code_min_temp_c_dense_rank"),
        F.rank().over(w9).alias("pressure_rank"),
        F.cume_dist().over(w10).alias("humidity_share_cume_dist"),
    )
)
