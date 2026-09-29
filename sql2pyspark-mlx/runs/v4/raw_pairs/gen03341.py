from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("reading_ts", "id")
w2 = Window.partitionBy("device_code").orderBy("reading_ts", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device_code").orderBy("prev_reading_ts", "id").rowsBetween(-2, -1)
w4 = Window.orderBy("is_new_burst", "id")
w5 = Window.partitionBy("device_code").orderBy(F.col("prev_reading_ts").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

filtered = (
    df_device_readings
    .filter((F.col("reading_ts") > F.to_date(F.lit("2025-09-01"))) & F.col("device_code").isin(["DV-04", "DV-03"]))
    .select(
        "id",
        "device_code",
        "reading_ts",
        "pressure",
        "status_bits",
        "firmware",
        (F.col("humidity") + F.col("temp_c")).alias("humidity_plus_temp_c"),
    )
)

prev_events = filtered.withColumn("prev_reading_ts", F.lag("reading_ts").over(w))

with_gaps = (
    prev_events
    .withColumn("is_new_burst", F.when(F.col("prev_reading_ts").isNull() | (F.col("reading_ts") > F.col("prev_reading_ts") + F.expr("INTERVAL 20 MINUTES")), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

scored = (
    islands
    .withColumn("humidity_plus_temp_c_avg_2_prior", F.avg("humidity_plus_temp_c").over(w3))
    .withColumn("prev_pressure", F.lag("pressure", 1).over(w4))
    .withColumn("island_id_max_30d", F.max("island_id").over(w5))
    .withColumn("next_island_id", F.lead("island_id", 1).over(w))
)

result = (
    scored
    .groupBy(
        F.when(F.col("island_id") >= 9, "LARGE").otherwise("SMALL").alias("island_id_tier"),
        F.date_trunc("QUARTER", "prev_reading_ts").alias("quarter_start"),
        "device_code",
    )
    .agg(
        F.sum(F.when(F.col("device_code") == "DV-01", F.col("pressure"))).alias("dv_01_total_pressure"),
        F.count_if(F.col("device_code") == "DV-01").alias("dv_01_events"),
    )
    .filter(F.col("dv_01_total_pressure") < 0)
    .orderBy(F.col("dv_01_events").desc())
)
