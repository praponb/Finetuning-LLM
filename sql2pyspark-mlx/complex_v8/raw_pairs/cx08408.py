from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.when(F.col("voltage") >= 235.0, "large").when(F.col("voltage") >= 223.3, "medium").otherwise("small").alias("voltage_tier"),
    )
)

with_prev = filtered.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_site_id_running))

result = (
    sessionized
    .filter(F.to_date(F.col("read_at")) < F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        "voltage_tier",
        "prev_read_at",
        "session_id",
        F.date_format(F.col("read_at"), "yyyy-MM").alias("read_at_label"),
    )
    .orderBy(F.col("kwh").asc())
)
