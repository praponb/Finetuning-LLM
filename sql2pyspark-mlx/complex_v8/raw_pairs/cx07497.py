from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

with_prev = cleaned.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_site_id_running))

result = (
    sessionized
    .filter(F.col("session_id").between(2, 6))
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        "kwh_bucket",
        "prev_read_at",
        "starts_session",
    )
    .orderBy(F.col("quality").asc_nulls_last(), F.col("site_id").desc(), "id")
    .limit(5)
)
