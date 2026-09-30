from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("new_group_flag", "id")
w4 = Window.partitionBy("device_code").orderBy(F.col("new_group_flag").desc_nulls_first())
w5 = Window.partitionBy("device_code").orderBy(F.col("new_group_flag_cume_dist").desc_nulls_last(), "id")
w6 = Window.partitionBy("device_code").orderBy(F.col("reading_ts").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w7 = Window.orderBy("new_group_flag_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_device_readings
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

scored = (
    sessions_device_readings
    .withColumn("prev_session_no", F.lag("session_no").over(w3))
    .withColumn("next_session_no", F.lead("session_no").over(w3))
    .withColumn("next_humidity", F.lead("humidity").over(w3))
    .withColumn("prev_humidity", F.lag("humidity").over(w3))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("new_group_flag_cume_dist", F.cume_dist().over(w4))
)

ranked = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "status_bits",
        "firmware",
        "session_no",
        "next_session_no",
        "next_humidity",
        "prev_humidity",
        "new_group_flag_cume_dist",
    )
)

enriched = (
    ranked
    .filter(F.col("new_group_flag_cume_dist") <= 0.33)
    .select(
        "*",
        F.count("next_humidity").over(w6).alias("next_humidity_count_30m"),
        F.first("new_group_flag_cume_dist", ignorenulls=True).over(w7).alias("first_known_new_group_flag_cume_dist"),
    )
)

gaps_2 = (
    enriched
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = gaps_2.withColumn("burst_id", F.sum("new_group_flag").over(w2))

valid = (
    sessionized
    .filter(F.col("next_session_no") < 7)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "status_bits",
        "firmware",
        "session_no",
        "next_session_no",
        "next_humidity",
        "prev_humidity",
        "new_group_flag_cume_dist",
        "first_known_new_group_flag_cume_dist",
        "new_group_flag",
        "burst_id",
        (F.col("new_group_flag") + F.col("pressure")).alias("new_group_flag_plus_pressure"),
    )
)

matched_keys = (
    valid
    .select("firmware", "device_code")
    .subtract(
        df_device_readings
        .filter(F.col("reading_ts") < "2025-03-01")
        .select("firmware", "device_code")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.firmware") == F.col("k.firmware")) & (F.col("s.device_code") == F.col("k.device_code")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("hour", "reading_ts").alias("hour_window"))
    .agg(
        F.count(F.when(F.col("device_code") == "DV-02", 1)).alias("dv_02_count"),
        ((F.unix_timestamp(F.max("reading_ts")) - F.unix_timestamp(F.min("reading_ts"))) / 60).alias("span_minutes"),
        F.count_if(F.col("device_code") == "DV-01").alias("dv_01_events"),
        F.bit_or("status_bits").alias("status_bits_bit_or"),
        F.min("first_known_new_group_flag_cume_dist").alias("min_first_known_new_group_flag_cume_dist"),
    )
    .orderBy("hour_window")
)
