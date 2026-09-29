from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy(F.col("new_group_flag").desc())

valid = (
    df_device_readings
    .filter(F.col("temp_c") <= 24)
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "humidity",
        "status_bits",
        "firmware",
        (F.col("pressure") * F.col("temp_c")).alias("pressure_times_temp_c"),
        F.when(F.col("humidity") >= 79, "High").when(F.col("humidity") >= 28, "Medium").otherwise("Low").alias("humidity_level"),
    )
)

gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 30 MINUTES"), 1).otherwise(0))
)

sessions_device_readings = gaps.withColumn("island_id", F.sum("session_start_flag").over(w2))

with_gaps = (
    sessions_device_readings
    .withColumn("new_group_flag", F.when(F.lag("reading_ts").over(w).isNull() | (F.col("reading_ts") > F.lag("reading_ts").over(w) + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "device_code",
        "reading_ts",
        "temp_c",
        "pressure_times_temp_c",
        "humidity_level",
        "island_id",
        F.sum("new_group_flag").over(w2).alias("island_id_2"),
    )
)

with_gaps_2 = (
    islands
    .withColumn("new_group_flag", F.when(F.col("reading_ts") - F.lag("reading_ts").over(w) > F.expr("INTERVAL 45 MINUTES"), 1).otherwise(0))
)

islands_device_readings = with_gaps_2.withColumn("island_id_3", F.sum("new_group_flag").over(w2))

ranked = (
    islands_device_readings
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

kept_keys = (
    ranked
    .select("id")
    .intersect(
        df_device_readings
        .filter(F.col("device_code").isin(["DV-03", "DV-04", "DV-01"]))
        .select("id")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "reading_ts",
        "temp_c",
        "pressure_times_temp_c",
        "humidity_level",
        "island_id",
        "island_id_3",
    )
    .orderBy(F.col("reading_ts").desc())
)
