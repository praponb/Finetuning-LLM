from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at_dayof = Window.partitionBy("event_type").orderBy(F.col("event_at_dayofweek").desc())
w_ticket_id = Window.partitionBy("ticket_id")
w_event_type_by_event_at_dayof_2 = Window.partitionBy("event_type").orderBy(F.col("event_at_dayofweek").desc(), "id").rowsBetween(-6, -1)
w_event_type_by_event_at_dayof_3 = Window.partitionBy("event_type").orderBy(F.col("event_at_dayofweek").desc(), "id")
w_latest = Window.partitionBy("event_type").orderBy("event_at_hour_sum_6_prior", "id")

recent_events = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.col("event_type") == "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
        F.hour(F.col("event_at")).alias("event_at_hour"),
        F.substring(F.trim(F.col("event_type")), 1, 2).alias("event_type_prefix"),
    )
)

ranked = (
    recent_events
    .select(
        "id",
        "event_at",
        "event_type",
        "event_at_hour",
        F.dense_rank().over(w_event_type_by_event_at_dayof).alias("event_at_dayofweek_dense_rank"),
        F.size(F.collect_set("actor").over(w_ticket_id)).alias("distinct_actor_in_ticket_id"),
        F.sum("event_at_hour").over(w_event_type_by_event_at_dayof_2).alias("event_at_hour_sum_6_prior"),
        F.round(F.cume_dist().over(w_event_type_by_event_at_dayof_3), 3).alias("event_at_dayofweek_cume_dist"),
    )
)

top_per_group = (
    ranked
    .filter((F.col("event_at_dayofweek_dense_rank") <= 5) & (F.col("event_at_hour") >= 8))
    .drop("event_at_dayofweek_dense_rank")
)

deduped = (
    top_per_group
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

per_group = (
    deduped
    .groupBy("event_type")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    per_group
    .select(
        "event_type",
        "n_rows",
        "distinct_ids",
        (F.floor(F.col("n_rows") / 2) * 2).alias("n_rows_bucket"),
    )
    .orderBy(F.col("event_type").desc())
)
