from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
    )
)

with_prev = valid.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_agent_id_running))

session_summary = (
    sessionized
    .groupBy("agent_id", "session_no")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("csat").alias("session_csat"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

annotated = (
    session_summary
    .select(
        "*",
        F.year(F.col("session_end")).alias("session_end_year"),
        F.coalesce(F.col("session_csat"), F.lit(0)).alias("session_csat_filled"),
        F.to_date(F.col("session_start")).alias("session_start_date"),
    )
)

stats = (
    annotated
    .groupBy(
        F.year("session_start_date").alias("session_start_date_year"),
        F.to_date("session_start").alias("session_start_day"),
        F.date_format("session_end", "yyyy-MM").alias("session_end_ym"),
    )
    .agg(
        F.round(F.stddev("session_csat_filled"), 2).alias("stddev_session_csat_filled"),
        F.min("session_start").alias("first_session_start"),
        F.max("session_start").alias("last_session_start"),
        F.min("session_end").alias("first_session_end"),
        F.sum("session_end_year").alias("sum_session_end_year_check"),
    )
    .filter(F.col("sum_session_end_year_check") > 2024)
    .drop("sum_session_end_year_check")
)

result = (
    stats
    .select(
        "session_start_date_year",
        "session_start_day",
        "session_end_ym",
        "first_session_start",
        "last_session_start",
        "first_session_end",
        F.date_trunc("day", F.col("first_session_start")).alias("first_session_start_day"),
    )
    .orderBy(F.col("last_session_start").desc_nulls_first())
)
