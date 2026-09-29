from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_quality = Window.partitionBy("quality").orderBy("read_at", "id")
w_quality_running = Window.partitionBy("quality").orderBy("read_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned_readings = (
    df_meter_readings
    .select(
        "id",
        "site_id",
        "read_at",
        "kwh",
        "voltage",
        "quality",
        F.quarter(F.col("read_at")).alias("read_at_quarter"),
    )
)

with_prev = cleaned_readings.withColumn("prev_read_at", F.lag("read_at").over(w_quality))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_read_at").isNull() | (F.col("read_at") > F.col("prev_read_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_quality_running))

sessions = (
    sessionized
    .groupBy("quality", "session_no")
    .agg(
        F.min("read_at").alias("session_start"),
        F.max("read_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("voltage").alias("session_voltage"),
        F.round((F.unix_timestamp(F.max("read_at")) - F.unix_timestamp(F.min("read_at"))) / 60, 1).alias("duration_min"),
    )
)

labeled = (
    sessions
    .filter(F.to_date(F.col("session_start")) < F.lit("2025-02-05").cast("date"))
    .select(
        "quality",
        "session_no",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        F.round(F.col("duration_min") * 1.1, 2).alias("duration_min_adj"),
        F.round(F.col("duration_min") * 1.07, 2).alias("duration_min_adj_2"),
        F.initcap(F.trim(F.col("quality"))).alias("quality_clean"),
    )
)

tagged = (
    labeled
    .filter(F.month(F.col("session_start")).isin(1, 2))
    .select(
        "*",
        F.least(F.col("duration_min_adj"), F.col("duration_min_adj_2")).alias("least_duration_min_adj_duration_min_adj_2"),
        (F.col("n_events") - F.col("duration_min")).alias("n_events_minus_duration_min"),
        F.date_trunc("day", F.col("session_end")).alias("session_end_day"),
    )
)

result = (
    tagged
    .select(
        "quality",
        "session_no",
        "duration_min_adj_2",
        "least_duration_min_adj_duration_min_adj_2",
        "duration_min_adj",
        "n_events_minus_duration_min",
    )
    .unpivot(
        ids=["quality", "session_no"],
        values=["duration_min_adj_2", "least_duration_min_adj_duration_min_adj_2", "duration_min_adj", "n_events_minus_duration_min"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .orderBy("quality", "session_no", "field")
)
