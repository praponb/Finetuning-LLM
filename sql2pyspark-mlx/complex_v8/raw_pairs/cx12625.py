from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_meter_readings
    .filter(F.col("kwh").between(42.1, 109.0))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        (F.floor(F.col("kwh") / 25) * 25).alias("kwh_bucket"),
    )
)

with_prev = base.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_site_id_running))

result = (
    sessionized
    .filter(F.col("new_session_flag") <= 1)
    .select(
        "id",
        "site_id",
        "kwh",
        "voltage",
        "quality",
        "kwh_bucket",
        "prev_read_at",
        "new_session_flag",
        "session_no",
        F.round(F.col("kwh") * 1.07, 2).alias("kwh_adj"),
    )
    .orderBy(F.col("voltage").desc(), F.col("kwh").desc())
)
