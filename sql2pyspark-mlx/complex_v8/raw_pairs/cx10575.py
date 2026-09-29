from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_starts_session = Window.orderBy(F.col("starts_session").asc_nulls_last(), "id")
w_quality_by_prev_read_at = Window.partitionBy("quality").orderBy(F.col("prev_read_at").desc(), "id")
w_quality_by_prev_read_at_ties = Window.partitionBy("quality").orderBy(F.col("prev_read_at").desc())
w_prev_read_at_month_site_id = Window.partitionBy(F.month("prev_read_at"), "site_id")

recent = (
    df_meter_readings
    .filter(F.col("kwh").between(45.0, 83.8))
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.to_date(F.col("read_at")).alias("read_at_date"),
    )
)

with_prev = recent.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_quality_running))

sequenced = (
    sessionized
    .select(
        "*",
        F.lead("burst_id").over(w_all_by_starts_session).alias("next_burst_id"),
        F.lag("prev_read_at").over(w_quality_by_prev_read_at).alias("prev_prev_read_at"),
        F.dense_rank().over(w_quality_by_prev_read_at_ties).alias("prev_read_at_dense_rank"),
        F.min("burst_id").over(w_prev_read_at_month_site_id).alias("prev_read_at_month_site_id_min_burst_id"),
    )
)

result = (
    sequenced
    .groupBy("read_at")
    .pivot("quality", ["BAD", "EST", "OK"])
    .agg(F.sum("kwh"))
    .orderBy("read_at")
)
