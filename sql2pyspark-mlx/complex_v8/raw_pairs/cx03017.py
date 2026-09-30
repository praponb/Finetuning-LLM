from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "quality",
        F.length(F.trim(F.col("quality"))).alias("quality_len"),
    )
)

with_prev = recent.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_quality_running))

result = (
    sessionized
    .groupingSets(
        [["quality", "starts_session", "session_no"], ["quality"], []],
        "quality", "starts_session", "session_no",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.stddev("quality_len"), 2).alias("stddev_quality_len"),
        F.min("quality_len").alias("min_quality_len"),
    )
    .orderBy(
        F.col("quality").asc_nulls_last(),
        F.col("starts_session").asc_nulls_last(),
        F.col("session_no").asc_nulls_last(),
    )
)
