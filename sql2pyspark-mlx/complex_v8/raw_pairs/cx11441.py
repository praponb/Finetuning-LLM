from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_tickets = (
    df_tickets
    .filter(F.col("opened_at") >= F.lit("2025-01-07 13:00:00").cast("timestamp"))
)

with_prev = prepared_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_agent_id_running))

result = (
    sessionized
    .cube("is_new_session", "csat", "agent_id")
    .agg(
        F.grouping("is_new_session").alias("is_total_is_new_session"),
        F.countDistinct("priority").alias("distinct_prioritys"),
    )
    .orderBy(
        F.col("is_new_session").asc_nulls_last(),
        F.col("csat").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
    )
)
