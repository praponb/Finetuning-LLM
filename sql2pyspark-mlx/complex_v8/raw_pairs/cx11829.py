from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), F.col("id").desc())

prepared = (
    df_ticket_events
    .filter(F.col("actor") == "system")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 7 DAYS")).alias("event_at_plus_7d"),
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
    )
)

tagged = (
    prepared
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        F.substring(F.trim(F.col("event_type")), 1, 2).alias("event_type_prefix"),
        F.date_trunc("hour", F.col("event_at_plus_7d")).alias("event_at_plus_7d_hour"),
        F.substring(F.trim(F.col("event_type")), 1, 4).alias("event_type_prefix_2"),
    )
)

deduped = (
    tagged
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .groupingSets(
        [["event_type", "ticket_id"], ["event_type"], []],
        "event_type", "ticket_id",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("event_type") == "assigned", 1)) / F.count("*"), 1).alias("pct_event_type_assigned"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
