from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_meter_readings
    .filter(F.col("voltage") <= 237.5)
    .select(
        "id",
        "site_id",
        "read_at",
        "voltage",
        "quality",
        F.substring(F.trim(F.col("quality")), 1, 4).alias("quality_prefix"),
    )
)

with_prev = valid.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_site_id_running))

per_group = (
    sessionized
    .groupBy(
        "session_no",
        F.date_trunc("month", "prev_read_at").alias("prev_read_at_month"),
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("site_id").alias("distinct_site_ids"),
    )
)

without_matches = (
    per_group
    .crossJoin(per_group.agg(F.avg("session_no").alias("overall_avg_session_no")))
    .filter(F.col("session_no") > F.col("overall_avg_session_no"))
    .drop("overall_avg_session_no")
)

result = (
    without_matches
    .select(
        "session_no",
        "prev_read_at_month",
        "n_rows",
        "distinct_site_ids",
        (F.col("n_rows") + F.col("distinct_site_ids")).alias("n_rows_plus_distinct_site_ids"),
        F.quarter(F.col("prev_read_at_month")).alias("prev_read_at_month_quarter"),
    )
    .orderBy(
        F.col("session_no").desc(),
        F.col("prev_read_at_month_quarter").desc_nulls_first(),
    )
)
