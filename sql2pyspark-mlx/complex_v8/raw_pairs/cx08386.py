from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_readings = (
    df_meter_readings
    .filter(F.col("quality").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

with_prev = scoped_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_quality_running))

result = (
    sessionized
    .filter(F.col("voltage") >= 215.7)
    .select(
        "id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "prev_read_at",
        "new_session_flag",
        "session_no",
        F.when(F.col("session_no") >= 4, "A").when(F.col("session_no") >= 3, "B").otherwise("C").alias("session_no_tier"),
    )
)
