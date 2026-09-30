from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
    )
)

with_prev = base_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_site_id_running))

enriched = (
    sessionized.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .filter(F.col("si.zone").isin("Z4", "Z3"))
    .select("s.*", F.col("si.zone").alias("site_zone"))
)

result = (
    enriched
    .select(
        "id",
        "kwh",
        "new_session_flag",
        "burst_id",
        "site_zone",
        F.when(F.col("new_session_flag") >= 1, "large").when(F.col("new_session_flag") >= 0, "medium").otherwise("small").alias("new_session_flag_tier"),
        F.substring(F.trim(F.col("site_zone")), 1, 4).alias("site_zone_prefix"),
    )
    .orderBy("kwh", F.col("new_session_flag_tier").asc_nulls_last())
)
