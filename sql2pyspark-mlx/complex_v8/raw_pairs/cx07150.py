from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id")
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_actor_by_event_at = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_actor_by_event_at_2 = Window.partitionBy("ticket_id", "actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_ticket_events
    .filter((F.hour(F.col("event_at")).between(6, 10)) & (F.col("actor") == "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
    )
)

metrics = (
    valid
    .select(
        "*",
        F.lead("event_at").over(w_event_type_by_event_at).alias("next_event_at"),
        F.nth_value("event_at", 2).over(w_actor_by_event_at_full).alias("second_event_at"),
        F.lead("event_at").over(w_ticket_id_actor_by_event_at).alias("next_event_at_2"),
        F.ntile(3).over(w_ticket_id_actor_by_event_at).alias("event_at_tercile"),
        F.round(F.percent_rank().over(w_event_type_by_event_at), 3).alias("event_at_percent_rank"),
        F.last("event_at").over(w_actor_by_event_at_full).alias("last_event_at"),
        F.dense_rank().over(w_event_type_by_event_at).alias("event_at_dense_rank"),
        F.nth_value("event_at", 2).over(w_ticket_id_actor_by_event_at_2).alias("second_event_at_2"),
    )
)

derived = (
    metrics
    .filter(F.col("event_at_label").isNotNull())
    .select(
        "*",
        (F.col("event_at_tercile") - F.col("event_at_dense_rank")).alias("event_at_tercile_minus_event_at_dense_rank"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

summary = (
    derived
    .groupBy(
        F.to_date("next_event_at_2").alias("next_event_at_2_day"),
        "ticket_id",
        "event_at_tercile",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.countDistinct("actor").alias("distinct_actors"),
    )
)

result = (
    summary
    .filter(F.col("next_event_at_2_day").isNotNull())
    .select(
        "next_event_at_2_day",
        "event_at_tercile",
        "cnt",
        "distinct_actors",
        F.when(F.col("cnt") >= 9, "HIGH").when(F.col("cnt") >= 5, "MEDIUM").otherwise("LOW").alias("cnt_tier"),
    )
    .orderBy(
        F.col("cnt").desc_nulls_first(),
        F.col("event_at_tercile").desc_nulls_first(),
    )
)
