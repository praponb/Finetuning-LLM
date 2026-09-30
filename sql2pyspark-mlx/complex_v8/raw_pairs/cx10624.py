from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_readings = (
    df_meter_readings
    .filter(F.col("kwh") > 20.3)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.round(F.col("kwh") / F.nullif(F.col("voltage"), F.lit(0)), 3).alias("kwh_per_voltage"),
    )
)

with_prev = filtered_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_site_id_running))

enriched = (
    sessionized
    .filter(F.col("new_session_flag") < 1)
    .select(
        "*",
        F.coalesce(F.col("quality"), F.lit("UNKNOWN")).alias("quality_or_unknown"),
    )
)

stats = (
    enriched
    .groupBy("site_id")
    .agg(
        F.sum(F.when(~F.col("quality").isin("EST", "BAD"), 1).otherwise(0)).alias("n_quality_not_in_est_bad"),
        F.round(100 * F.count(F.when(F.col("quality") != "OK", 1)) / F.count("*"), 1).alias("pct_quality_ok"),
        F.min("new_session_flag").alias("min_new_session_flag"),
        F.max("read_at").alias("last_read_at"),
        F.round(F.avg("kwh_per_voltage"), 2).alias("avg_kwh_per_voltage"),
    )
)

result = (
    stats
    .select(
        "site_id",
        "n_quality_not_in_est_bad",
        "pct_quality_ok",
        "last_read_at",
        "avg_kwh_per_voltage",
    )
)
