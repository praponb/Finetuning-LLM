from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc_nulls_first())
w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy("event_at", "id")

prepared = (
    df_ticket_events
    .filter(F.hour(F.col("event_at")).between(9, 15))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
    )
)

metrics = (
    prepared
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_type_ref",
        F.round(F.cume_dist().over(w_ticket_id_event_type_by_even), 3).alias("event_at_cume_dist"),
        F.nth_value("event_at", 2).over(w_ticket_id_by_event_at_full).alias("second_event_at"),
        F.last("event_at").over(w_ticket_id_by_event_at_full).alias("last_event_at"),
        F.row_number().over(w_event_type_ticket_id_by_even).alias("rn"),
        F.lag("event_at", 2).over(w_event_type_ticket_id_by_even).alias("prev2_event_at"),
    )
)

result = (
    metrics
    .groupingSets(
        [["event_type", "ticket_id", "rn"], ["event_type"], ["ticket_id"], []],
        "event_type", "ticket_id", "rn",
    )
    .agg(
        F.grouping("event_type").alias("is_total_event_type"),
        F.sum("event_at_cume_dist").alias("total_event_at_cume_dist"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("actor").isin("customer", "agent"), F.col("rn"))).alias("rn_actor_in_customer_agent"),
    )
    .select(
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "ticket_id",
        "rn",
        "is_total_event_type",
        "total_event_at_cume_dist",
        "cnt",
        "rn_actor_in_customer_agent",
    )
    .orderBy(
        F.col("event_type").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
        F.col("rn").asc_nulls_last(),
    )
)
