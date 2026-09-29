from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_or_unknown_event_at_mo = Window.partitionBy("actor_or_unknown", F.month("event_at")).orderBy("event_at")
w_all_by_event_at_ties = Window.orderBy("event_at")
w_actor_or_unknown_event_at_mo_2 = Window.partitionBy("actor_or_unknown", F.month("event_at")).orderBy("event_at", "id")
w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at = Window.orderBy(F.col("event_at").asc_nulls_last(), "id")
w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy(F.col("event_at").desc())
w_event_type_ticket_id_by_even_2 = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").desc(), "id")
w_actor_by_event_at_full = Window.partitionBy("actor").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_ticket_events
    .filter((F.col("event_type").isin("created", "escalated", "resolved")) & (F.col("actor").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

ranked = (
    cleaned
    .select(
        "*",
        F.round(F.percent_rank().over(w_actor_or_unknown_event_at_mo), 3).alias("event_at_percent_rank"),
        F.dense_rank().over(w_all_by_event_at_ties).alias("event_at_dense_rank"),
        F.lead("event_at", 2).over(w_actor_or_unknown_event_at_mo_2).alias("next2_event_at"),
        F.nth_value("event_at", 2).over(w_event_type_ticket_id_by_even).alias("second_event_at"),
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist"),
        F.round(F.percent_rank().over(w_actor_by_event_at_ties), 3).alias("event_at_percent_rank_2"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_type_ticket_id_by_even_2))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.cume_dist().over(w_event_type_ticket_id_by_even_2), 3).alias("event_at_cume_dist_2"),
        F.last("event_at").over(w_actor_by_event_at_full).alias("last_event_at"),
        F.ntile(3).over(w_all_by_event_at).alias("event_at_tercile"),
        F.round(F.percent_rank().over(w_actor_or_unknown_event_at_mo_2), 3).alias("event_at_percent_rank_3"),
    )
)

annotated = (
    ranked
    .select(
        "*",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len_2"),
    )
)

grouped = (
    annotated
    .filter(F.col("event_at_percent_rank_3").between(0.1, 0.8))
    .groupBy(
        "event_type_len",
        F.to_date("last_event_at").alias("last_event_at_day"),
        "event_type",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("actor") == "customer", 1)) / F.count("*"), 1).alias("pct_actor_customer"),
        F.count("*").alias("row_count"),
        F.countDistinct("actor_or_unknown").alias("distinct_actor_or_unknowns"),
    )
)

result = (
    grouped
    .select(
        "event_type_len",
        "last_event_at_day",
        "pct_actor_customer",
        "distinct_actor_or_unknowns",
        F.date_format(F.col("last_event_at_day"), "yyyy-MM-dd").alias("last_event_at_day_label"),
        F.date_trunc("month", F.col("last_event_at_day")).alias("last_event_at_day_month"),
    )
    .orderBy(F.col("pct_actor_customer").asc_nulls_last())
)
