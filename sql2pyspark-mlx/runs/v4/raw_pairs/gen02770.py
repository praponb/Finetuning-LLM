from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())

with_gaps = (
    df_sessions
    .withColumn("session_start_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

with_latest = (
    sessionized
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec"))
)

with_gaps_2 = (
    with_latest
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_sessions = with_gaps_2.withColumn("session_no_2", F.sum("is_new_burst").over(w2))

kept = (
    sessions_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.event_time") >= F.col("s.started_at")), "left_anti")
)

result = (
    kept
    .withColumn("session_no_2_tier", F.when(F.col("session_no_2") >= 6, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["session_no_2_tier", "browser"], ["session_no_2_tier"], []],
        "session_no_2_tier",
        "browser",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum("session_start_flag") / F.nullif(F.count(F.when(F.col("browser") == "Edge", 1)), F.lit(0)), 2).alias("session_start_flag_per_edge"),
    )
    .orderBy("browser")
)
