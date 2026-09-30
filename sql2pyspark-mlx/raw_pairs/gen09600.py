from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("country").orderBy("started_at", "id")
w4 = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w5 = Window.orderBy(F.col("started_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w6 = Window.orderBy("prev_session_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("next_session_id").desc(), "id")
w8 = Window.orderBy("next_session_id", "id")
w_part = Window.orderBy("seq_no", "id")
w10 = Window.orderBy("last_known_seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter(F.col("browser").isNotNull())
    .select("*", F.trim("country").alias("clean_country"))
)

prev_events = base.withColumn("prev_started_at", F.lag("started_at").over(w))

with_gaps = (
    prev_events
    .withColumn("session_start_flag", F.when(F.col("prev_started_at").isNull() | (F.col("started_at") > F.col("prev_started_at") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_sessions = (
    with_gaps
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        "clean_country",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

enriched = (
    sessions_sessions
    .select(
        "id",
        "started_at",
        "clean_country",
        F.lead("session_id").over(w_grp).alias("next_session_id"),
        F.lag("session_id").over(w_grp).alias("prev_session_id"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

metrics = (
    enriched
    .filter(F.col("seq_no").between(5, 6))
    .select(
        "id",
        "clean_country",
        "seq_no",
        F.sum("seq_no").over(w4).alias("seq_no_sum_90d"),
        F.max("prev_session_id").over(w5).alias("prev_session_id_max_30d"),
        F.last("seq_no", ignorenulls=True).over(w6).alias("last_known_seq_no"),
        F.row_number().over(w7).alias("event_seq"),
        F.lag("seq_no", 1).over(w8).alias("prev_seq_no"),
    )
)

enriched_sessions = (
    metrics
    .select(
        "id",
        "clean_country",
        "seq_no",
        "seq_no_sum_90d",
        "prev_session_id_max_30d",
        F.lead("seq_no").over(w_part).alias("next_seq_no"),
        F.lag("seq_no").over(w_part).alias("prev_seq_no_2"),
        F.lag("last_known_seq_no").over(w_part).alias("prev_last_known_seq_no"),
        F.row_number().over(w_part).alias("row_seq"),
        F.sum("prev_session_id_max_30d").over(w10).alias("cumulative_prev_session_id_max_30d"),
    )
)

result = (
    enriched_sessions
    .select(
        "id",
        "seq_no_sum_90d",
        "prev_session_id_max_30d",
        "next_seq_no",
        "prev_last_known_seq_no",
        "row_seq",
        "cumulative_prev_session_id_max_30d",
    )
    .orderBy(F.col("prev_session_id_max_30d").desc())
)
