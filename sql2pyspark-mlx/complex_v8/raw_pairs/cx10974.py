from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_ticket_id_by_event_at_2 = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id")
w_all_by_event_at_ties = Window.orderBy("event_at")
w_event_type_group_by_event_at = Window.partitionBy("event_type_group").orderBy(F.col("event_at").desc(), "id")
w_event_at_to_date_actor_by_ev = Window.partitionBy(F.to_date("event_at"), "actor").orderBy(F.col("event_at").desc(), "id")

prepared_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("assigned", "created"), "group_a").when(F.col("event_type") == "replied", "group_b").otherwise("other").alias("event_type_group"),
        F.initcap(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

with_windows = (
    prepared_events
    .select(
        "*",
        F.lag("event_at").over(w_ticket_id_by_event_at).alias("prev_event_at"),
        F.ntile(4).over(w_ticket_id_by_event_at_2).alias("event_at_quartile"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_by_event_at))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_ticket_id_by_event_at_2))) / 60, 1).alias("minutes_since_prev_event_at_2"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.ntile(4).over(w_event_type_group_by_event_at).alias("event_at_quartile_2"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_at_to_date_actor_by_ev))) / 60, 1).alias("minutes_since_prev_event_at_3"),
    )
)

result = (
    with_windows
    .filter(F.col("event_at_quartile").between(4, 7))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_type_group",
        "event_type_clean",
        "prev_event_at",
        "event_at_quartile",
        "event_at_dense_rank",
        "event_at_quartile_2",
        (F.floor(F.col("event_at_quartile_2") / 2) * 2).alias("event_at_quartile_2_bucket"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("event_at_dense_rank").cast("string")).alias("event_type_ref"),
    )
)
