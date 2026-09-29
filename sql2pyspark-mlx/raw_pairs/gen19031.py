from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("device_code")
w5 = Window.partitionBy(F.date_trunc("MONTH", "reading_ts"), "device_code").orderBy("temp_c", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flagged = (
    df_device_readings
    .withColumn("is_new_session", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("is_new_session").over(w2))

scored = (
    islands
    .filter(F.col("temp_c") >= 27)
    .select(
        "id",
        "device_code",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "island_id",
        F.avg("temp_c").over(w_grp).alias("temp_c_avg_6_prior"),
        F.min("humidity").over(w4).alias("device_code_min_humidity"),
        F.round((F.col("humidity") / F.sum("humidity").over(w4)), 4).alias("humidity_share"),
        F.first("humidity", ignorenulls=True).over(w5).alias("first_known_humidity"),
    )
)

base = (
    scored
    .filter(F.col("first_known_humidity") >= 65)
    .select(
        "id",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        "device_code_min_humidity",
        "humidity_share",
        "first_known_humidity",
        F.substring("device_code", 1, 3).alias("device_code_prefix"),
    )
)

kept_keys = (
    base
    .select("id", "firmware")
    .intersectAll(
        df_device_readings
        .filter(F.hour("reading_ts").between(6, 11))
        .select("id", "firmware")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.firmware") == F.col("k.firmware")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("humidity_share") >= 0.69, "PREMIUM").otherwise("STANDARD").alias("humidity_share_tier"),
    )
    .agg(
        F.sum("status_bits").alias("total_status_bits"),
        F.count("*").alias("num_rows"),
        F.bit_and("status_bits").alias("status_bits_bit_and"),
    )
)
