from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.hour(F.col("read_at")).alias("read_at_hour"),
    )
)

with_prev = filtered.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_quality_running))

screened = (
    sessionized
    .crossJoin(sessionized.agg(F.max("read_at_hour").alias("overall_max_read_at_hour")))
    .filter(F.col("read_at_hour") < F.col("overall_max_read_at_hour"))
    .drop("overall_max_read_at_hour")
)

result = (
    screened
    .select(
        "id",
        "kwh",
        "voltage",
        "quality",
        "read_at_hour",
        "prev_read_at",
        "is_new_session",
        "burst_id",
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
    .orderBy(F.col("quality").desc(), "quality_clean")
)
