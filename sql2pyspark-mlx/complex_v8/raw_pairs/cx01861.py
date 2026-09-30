from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_meter_readings
    .filter(F.col("site_id").isNotNull())
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_prev = cleaned.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_quality_running))

tagged = (
    sessionized
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "prev_read_at",
        "is_new_session",
        "burst_id",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
        F.date_trunc("month", F.col("prev_read_at")).alias("prev_read_at_month"),
        F.greatest(F.col("burst_id"), F.col("kwh")).alias("greatest_burst_id_kwh"),
    )
)

result = (
    tagged
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        "prev_read_at",
        "is_new_session",
        "burst_id",
        "quality_prefix",
        "prev_read_at_month",
        "greatest_burst_id_kwh",
        F.initcap(F.trim(F.col("quality_prefix"))).alias("quality_prefix_clean"),
        (F.col("kwh") - F.col("voltage")).alias("kwh_minus_voltage"),
    )
)
