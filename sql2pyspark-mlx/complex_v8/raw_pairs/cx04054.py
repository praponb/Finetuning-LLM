from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.upper(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

with_prev = scoped.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_site_id_running))

result = (
    sessionized
    .groupingSets(
        [["site_id", "session_no", "quality"], ["site_id"]],
        "site_id", "session_no", "quality",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.min("kwh").alias("min_kwh"),
        F.round(F.avg("is_new_session"), 2).alias("avg_is_new_session"),
    )
    .orderBy(
        F.col("site_id").asc_nulls_last(),
        F.col("session_no").asc_nulls_last(),
        F.col("quality").asc_nulls_last(),
    )
)
