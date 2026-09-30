from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_site_id = Window.partitionBy("site_id").orderBy("read_at", "id")
w_site_id_running = Window.partitionBy("site_id").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_site_id_burst_id_by_session = Window.partitionBy("site_id", "burst_id").orderBy(F.col("session_end").desc_nulls_first(), "site_id", "burst_id")
w_session_end_to_date = Window.partitionBy(F.to_date("session_end"))
w_n_events_session_start_to_da = Window.partitionBy("n_events", F.to_date("session_start")).orderBy(F.col("n_events").desc(), "site_id", "burst_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_readings = (
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

with_prev = base_readings.withColumn("prev_read_at", F.lag("read_at").over(w_site_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | ((F.unix_timestamp("read_at") - F.unix_timestamp("prev_read_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_site_id_running))

per_session = (
    sessionized
    .groupBy("site_id", "burst_id")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

with_trends = (
    per_session
    .select(
        "*",
        F.rank().over(w_site_id_burst_id_by_session).alias("session_end_rank"),
        F.count(F.lit(1)).over(w_session_end_to_date).alias("rows_in_session_end_to_date"),
        F.sum("n_events").over(w_n_events_session_start_to_da).alias("running_n_events"),
        F.lead("n_events").over(w_site_id_burst_id_by_session).alias("next_n_events"),
    )
)

result = (
    with_trends
    .select(
        "site_id",
        "burst_id",
        F.col("rows_in_session_end_to_date").alias("rows_in_session_end_to_date_v"),
        F.col("running_n_events").alias("running_n_events_v"),
        F.col("n_events").alias("m_n_events"),
        F.col("next_n_events").alias("m_next_n_events"),
    )
    .unpivot(
        ids=["site_id", "burst_id"],
        values=["rows_in_session_end_to_date_v", "running_n_events_v", "m_n_events", "m_next_n_events"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("site_id", "burst_id", "field")
)
