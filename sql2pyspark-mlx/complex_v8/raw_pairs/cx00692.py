from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id")
w_ticket_id_event_type_by_even_2 = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_id_event_type_by_even_3 = Window.partitionBy("ticket_id", "event_type").orderBy("event_at")
w_event_type_event_at_tercile = Window.partitionBy("event_type", "event_at_tercile").orderBy("first_prev3_event_at", "event_type", "prev3_event_at_month", "event_at_tercile")
w_event_at_tercile_first_prev3 = Window.partitionBy("event_at_tercile", F.month("first_prev3_event_at")).orderBy("n_actor_in_agent_customer", "event_type", "prev3_event_at_month", "event_at_tercile")

base = (
    df_ticket_events
    .filter(F.col("event_type").isin("escalated", "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

ranked = (
    base
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        F.ntile(3).over(w_ticket_id_event_type_by_even).alias("event_at_tercile"),
        F.lag("event_at").over(w_ticket_id_event_type_by_even).alias("prev_event_at"),
        F.first("event_at").over(w_ticket_id_event_type_by_even_2).alias("first_event_at"),
        F.dense_rank().over(w_ticket_id_event_type_by_even).alias("event_at_dense_rank"),
        F.lag("event_at", 3).over(w_ticket_id_event_type_by_even).alias("prev3_event_at"),
        F.dense_rank().over(w_ticket_id_event_type_by_even_3).alias("event_at_dense_rank_2"),
    )
)

grouped = (
    ranked
    .groupBy(
        "event_type",
        F.date_trunc("month", "prev3_event_at").alias("prev3_event_at_month"),
        "event_at_tercile",
    )
    .agg(
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.sum("event_at_dense_rank_2").alias("total_event_at_dense_rank_2"),
        F.sum(F.when(F.col("actor").isin("agent", "customer"), 1).otherwise(0)).alias("n_actor_in_agent_customer"),
        F.min("prev3_event_at").alias("first_prev3_event_at"),
    )
)

with_trends = (
    grouped
    .select(
        "*",
        F.dense_rank().over(w_event_type_event_at_tercile).alias("first_prev3_event_at_dense_rank"),
        F.rank().over(w_event_at_tercile_first_prev3).alias("n_actor_in_agent_customer_rank"),
    )
)

result = (
    with_trends
    .filter(F.col("n_actor_in_agent_customer_rank") >= 3)
    .select(
        "event_type",
        "prev3_event_at_month",
        "actor_list",
        "total_event_at_dense_rank_2",
        "n_actor_in_agent_customer",
        "first_prev3_event_at",
        "first_prev3_event_at_dense_rank",
        "n_actor_in_agent_customer_rank",
    )
    .orderBy(
        F.col("n_actor_in_agent_customer").desc_nulls_first(),
        "n_actor_in_agent_customer_rank",
    )
)
