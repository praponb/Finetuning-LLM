from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_type_len = Window.partitionBy("actor").orderBy("event_type_len", "id")
w_event_type_event_type_len = Window.partitionBy("event_type", "event_type_len")
w_actor_event_at_year_by_ticke = Window.partitionBy("actor", F.year("event_at")).orderBy(F.col("ticket_closed_at").desc())
w_actor_by_event_type_len_ties = Window.partitionBy("actor").orderBy("event_type_len")
w_actor_by_ticket_closed_at_mo = Window.partitionBy("actor").orderBy(F.col("ticket_closed_at").desc(), "id").rowsBetween(-3, Window.currentRow)
w_actor_by_ticket_closed_at = Window.partitionBy("actor").orderBy(F.col("ticket_closed_at").desc(), "id")
w_event_type_event_at_year = Window.partitionBy("event_type", F.year("event_at"))
w_all_by_event_type_len_change = Window.orderBy(F.col("event_type_len_change_2").desc(), "id")
w_event_type_len_decile_by_row = Window.partitionBy("event_type_len_decile").orderBy("rows_in_event_type_event_type_len", "id")
w_all_by_event_type_len_change_2 = Window.orderBy(F.col("event_type_len_change_2").desc(), "id").rowsBetween(-6, Window.currentRow)
w_event_type_len_decile_by_row_2 = Window.partitionBy("event_type_len_decile").orderBy("rows_in_event_type_event_type_len", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_events = (
    df_ticket_events
    .filter((F.col("event_type") == "assigned") & (F.col("actor") == "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
    )
)

joined = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select("s.*", F.col("t.closed_at").alias("ticket_closed_at"))
)

sequenced = (
    joined
    .select(
        "id",
        "event_at",
        "event_type",
        "actor",
        "event_type_len",
        "ticket_closed_at",
        F.ntile(10).over(w_actor_by_event_type_len).alias("event_type_len_decile"),
        F.size(F.collect_set("actor").over(w_event_type_event_type_len)).alias("distinct_actor_in_event_type_event_type_len"),
        F.min("event_type_len").over(w_event_type_event_type_len).alias("event_type_event_type_len_min_event_type_len"),
        F.round(F.col("event_type_len") - F.avg("event_type_len").over(w_event_type_event_type_len), 2).alias("event_type_len_vs_event_type_event_type_len_avg"),
        (F.col("event_type_len") - F.lag("event_type_len").over(w_actor_by_event_type_len)).alias("event_type_len_change"),
        F.round(F.cume_dist().over(w_actor_event_at_year_by_ticke), 3).alias("ticket_closed_at_cume_dist"),
        F.round(F.percent_rank().over(w_actor_by_event_type_len_ties), 3).alias("event_type_len_percent_rank"),
        F.round(F.avg("event_type_len").over(w_actor_by_ticket_closed_at_mo), 2).alias("event_type_len_avg_4"),
        F.round(F.cume_dist().over(w_actor_by_ticket_closed_at), 3).alias("ticket_closed_at_cume_dist_2"),
        (F.col("event_type_len") - F.lag("event_type_len").over(w_actor_by_ticket_closed_at)).alias("event_type_len_change_2"),
        F.count(F.lit(1)).over(w_event_type_event_type_len).alias("rows_in_event_type_event_type_len"),
    )
)

ranked = (
    sequenced
    .select(
        "*",
        F.round(F.col("ticket_closed_at_cume_dist") - F.avg("ticket_closed_at_cume_dist").over(w_event_type_event_at_year), 2).alias("ticket_closed_at_cume_dist_vs_event_type_event_a"),
        F.row_number().over(w_all_by_event_type_len_change).alias("rn"),
        (F.col("event_type_len_change") - F.lag("event_type_len_change").over(w_event_type_len_decile_by_row)).alias("event_type_len_change_change"),
        F.count(F.lit(1)).over(w_event_type_event_at_year).alias("rows_in_event_type_event_at_year"),
        (F.col("event_type_len_percent_rank") - F.lag("event_type_len_percent_rank").over(w_event_type_len_decile_by_row)).alias("event_type_len_percent_rank_change"),
        F.round(F.avg("ticket_closed_at_cume_dist").over(w_all_by_event_type_len_change_2), 2).alias("ticket_closed_at_cume_dist_avg_7"),
        F.count(F.lit(1)).over(w_event_type_len_decile_by_row_2).alias("running_count"),
    )
)

result = (
    ranked
    .select(
        "id",
        "event_type",
        "event_type_len",
        "ticket_closed_at",
        "event_type_event_type_len_min_event_type_len",
        "event_type_len_percent_rank",
        "rows_in_event_type_event_type_len",
        "ticket_closed_at_cume_dist_vs_event_type_event_a",
        "rn",
        "event_type_len_change_change",
        "event_type_len_percent_rank_change",
        "running_count",
        (F.col("event_type_len") - F.col("rn")).alias("event_type_len_minus_rn"),
        F.when(F.col("event_type_len_avg_4") >= 12.2, "A").when(F.col("event_type_len_avg_4") >= 6.2, "B").otherwise("C").alias("event_type_len_avg_4_tier"),
    )
)
