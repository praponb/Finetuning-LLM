from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_2 = Window.partitionBy("site_id")
w_session_end_month_site_zone = Window.partitionBy(F.month("session_end"), "site_zone").orderBy(F.col("duration_min").desc_nulls_first(), "site_id", "session_no")
w_all_by_session_end_running = Window.orderBy(F.col("session_end").desc(), "site_id", "session_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
    )
)

with_prev = filtered.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_site_id_running))

session_summary = (
    sessionized
    .groupBy("site_id", "session_no")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

with_parent = (
    session_summary.alias("s")
    .join(df_sites.alias("si"), F.col("si.id") == F.col("s.site_id"), "inner")
    .select(
        "s.site_id",
        "s.session_no",
        "s.session_end",
        "s.duration_min",
        "si.site_name",
        F.col("si.zone").alias("site_zone"),
    )
)

sequenced = (
    with_parent
    .select(
        "site_id",
        "session_no",
        "session_end",
        F.size(F.collect_set("site_zone").over(w_site_id_2)).alias("distinct_site_zone_in_site_id"),
        F.lag("duration_min").over(w_session_end_month_site_zone).alias("prev_duration_min"),
        F.last("session_no", ignorenulls=True).over(w_all_by_session_end_running).alias("last_known_session_no"),
    )
)

result = (
    sequenced
    .select(
        "site_id",
        "session_no",
        "last_known_session_no",
        "distinct_site_zone_in_site_id",
    )
    .unpivot(
        ids=["site_id", "session_no"],
        values=["last_known_session_no", "distinct_site_zone_in_site_id"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("site_id", "session_no", "measure")
)
