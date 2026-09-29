from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at_year = Window.orderBy(F.col("event_at_year").desc(), "id")
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc_nulls_first(), "id")
w_actor_ticket_id_by_event_at_3 = Window.partitionBy("actor", "ticket_id").orderBy("event_at")
w_all_by_event_at_year_ties = Window.orderBy(F.col("event_at_year").desc())
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc_nulls_first())
w_last_prev2_event_at_year_mon = Window.partitionBy(F.month("last_prev2_event_at_year")).orderBy("last_prev2_event_at_year", "event_at_year_month", "ticket_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_last_prev2_event_at_year_mon_2 = Window.partitionBy(F.month("last_prev2_event_at_year")).orderBy("last_prev2_event_at_year")
w_cnt = Window.partitionBy("cnt")

recent_events = (
    df_ticket_events
    .filter((F.col("event_type") != "created") & (F.month(F.col("event_at")).isin(1, 2)))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("year", F.col("event_at")).alias("event_at_year"),
    )
)

with_trends = (
    recent_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "event_at_year",
        F.ntile(3).over(w_all_by_event_at_year).alias("event_at_year_tercile"),
        F.nth_value("event_at", 2).over(w_actor_ticket_id_by_event_at).alias("second_event_at"),
        F.row_number().over(w_actor_ticket_id_by_event_at_2).alias("seq_no"),
        F.ntile(5).over(w_all_by_event_at).alias("event_at_quintile"),
        F.round(F.cume_dist().over(w_actor_ticket_id_by_event_at_3), 3).alias("event_at_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_event_at_year_ties), 3).alias("event_at_year_cume_dist"),
        F.ntile(10).over(w_all_by_event_at).alias("event_at_decile"),
        F.lag("event_at_year", 2).over(w_actor_ticket_id_by_event_at_2).alias("prev2_event_at_year"),
        F.rank().over(w_all_by_event_at_ties).alias("event_at_rank"),
        F.lead("event_at_year").over(w_all_by_event_at_year).alias("next_event_at_year"),
    )
)

derived = (
    with_trends
    .select(
        "*",
        (F.floor(F.col("event_at_year_tercile") / 2) * 2).alias("event_at_year_tercile_bucket"),
        F.date_format(F.col("event_at_year"), "yyyy-MM").alias("event_at_year_label"),
        (F.col("event_at_rank") + F.col("event_at_quintile")).alias("event_at_rank_plus_event_at_quintile"),
    )
)

per_group = (
    derived
    .filter(F.to_date(F.col("event_at_year")) < F.lit("2025-02-17").cast("date"))
    .groupBy(
        F.date_trunc("month", "event_at_year").alias("event_at_year_month"),
        "ticket_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("actor").isin("agent", "customer"), 1)).alias("n_actor_in_agent_customer"),
        F.max("prev2_event_at_year").alias("last_prev2_event_at_year"),
        F.max("seq_no").alias("max_seq_no"),
        F.max("next_event_at_year").alias("last_next_event_at_year"),
    )
    .filter(F.col("cnt") >= 2)
)

sequenced = (
    per_group
    .select(
        "event_at_year_month",
        "ticket_id",
        "n_actor_in_agent_customer",
        "last_prev2_event_at_year",
        "max_seq_no",
        "last_next_event_at_year",
        F.last("event_at_year_month").over(w_last_prev2_event_at_year_mon).alias("last_event_at_year_month"),
        F.rank().over(w_last_prev2_event_at_year_mon_2).alias("last_prev2_event_at_year_rank"),
        F.size(F.collect_set("ticket_id").over(w_cnt)).alias("distinct_ticket_id_in_cnt"),
    )
)

result = (
    sequenced
    .select(
        "event_at_year_month",
        "ticket_id",
        "n_actor_in_agent_customer",
        "last_prev2_event_at_year",
        "max_seq_no",
        "last_next_event_at_year",
        "last_event_at_year_month",
    )
    .orderBy(F.col("max_seq_no").desc_nulls_first(), "last_prev2_event_at_year")
)
