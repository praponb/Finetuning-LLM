from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_ticket_csat = Window.partitionBy("ticket_id", "ticket_csat")
w_actor_ticket_csat_by_ticket = Window.partitionBy("actor", "ticket_csat").orderBy(F.col("ticket_csat").desc())
w_all_by_ticket_csat_ties = Window.orderBy(F.col("ticket_csat").desc())
w_all_by_ticket_csat_running = Window.orderBy(F.col("ticket_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id = Window.partitionBy("ticket_id")
w_ticket_csat_by_ticket_csat_c = Window.partitionBy("ticket_csat").orderBy(F.col("ticket_csat_cume_dist").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_by_ticket_csat = Window.partitionBy("ticket_id").orderBy(F.col("ticket_csat").desc(), "id")
w_ticket_id_first_known_ticket = Window.partitionBy("ticket_id", "first_known_ticket_csat")
w_ticket_id_by_ticket_csat_ful = Window.partitionBy("ticket_id").orderBy(F.col("ticket_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ticket_csat_by_ticket_csat_c_2 = Window.partitionBy("ticket_csat").orderBy(F.col("ticket_csat_cume_dist").asc_nulls_last(), "id")
w_ticket_id_by_ticket_csat_tie = Window.partitionBy("ticket_id").orderBy(F.col("ticket_csat").desc())

recent = (
    df_ticket_events
    .filter((F.month(F.col("event_at")).isin(1, 2)) | (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
    )
)

enriched_ticket = (
    recent.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.actor_ref",
        F.col("t.csat").alias("ticket_csat"),
    )
)

windowed = (
    enriched_ticket
    .select(
        "*",
        F.max("ticket_csat").over(w_ticket_id_ticket_csat).alias("ticket_id_ticket_csat_max_ticket_csat"),
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_ticket_id_ticket_csat), F.lit(0)), 4).alias("ticket_csat_share_of_ticket_id_ticket_csat"),
        F.round(F.percent_rank().over(w_actor_ticket_csat_by_ticket), 3).alias("ticket_csat_percent_rank"),
        F.round(F.cume_dist().over(w_all_by_ticket_csat_ties), 3).alias("ticket_csat_cume_dist"),
        F.first("ticket_csat", ignorenulls=True).over(w_all_by_ticket_csat_running).alias("first_known_ticket_csat"),
        F.min("ticket_csat").over(w_ticket_id_ticket_csat).alias("ticket_id_ticket_csat_min_ticket_csat"),
        F.count(F.lit(1)).over(w_ticket_id_ticket_csat).alias("rows_in_ticket_id_ticket_csat"),
        F.max("ticket_csat").over(w_all_by_ticket_csat_running).alias("max_ticket_csat_so_far"),
        F.round(F.col("ticket_csat") / F.nullif(F.sum("ticket_csat").over(w_ticket_id), F.lit(0)), 4).alias("ticket_csat_share_of_ticket_id"),
        F.round(F.col("ticket_csat") - F.avg("ticket_csat").over(w_ticket_id_ticket_csat), 2).alias("ticket_csat_vs_ticket_id_ticket_csat_avg"),
    )
)

ranked = (
    windowed
    .select(
        "*",
        F.last("ticket_csat", ignorenulls=True).over(w_ticket_csat_by_ticket_csat_c).alias("last_known_ticket_csat"),
        F.round(F.cume_dist().over(w_ticket_id_by_ticket_csat), 3).alias("ticket_csat_cume_dist_2"),
        F.count(F.lit(1)).over(w_ticket_id_first_known_ticket).alias("rows_in_ticket_id_first_known_ticket_csat"),
        F.first("ticket_csat_cume_dist").over(w_ticket_id_by_ticket_csat_ful).alias("first_ticket_csat_cume_dist"),
        F.max("ticket_csat_cume_dist").over(w_ticket_id_first_known_ticket).alias("ticket_id_first_known_ticket_csat_max_ticket_csa"),
        F.row_number().over(w_ticket_csat_by_ticket_csat_c_2).alias("rn"),
        F.round(F.percent_rank().over(w_ticket_id_by_ticket_csat_tie), 3).alias("ticket_csat_percent_rank_2"),
        F.round(F.col("ticket_csat_cume_dist") / F.nullif(F.sum("ticket_csat_cume_dist").over(w_ticket_id_first_known_ticket), F.lit(0)), 4).alias("ticket_csat_cume_dist_share_of_ticket_id_first_k"),
    )
)

result = (
    ranked
    .filter(F.col("first_ticket_csat_cume_dist") < 0.6)
    .select(
        "id",
        "actor",
        "actor_ref",
        "ticket_csat",
        "ticket_csat_share_of_ticket_id_ticket_csat",
        "ticket_csat_percent_rank",
        "ticket_csat_cume_dist",
        "ticket_id_ticket_csat_min_ticket_csat",
        "max_ticket_csat_so_far",
        "ticket_csat_vs_ticket_id_ticket_csat_avg",
        "ticket_csat_cume_dist_2",
        "rows_in_ticket_id_first_known_ticket_csat",
        "first_ticket_csat_cume_dist",
        "rn",
        "ticket_csat_cume_dist_share_of_ticket_id_first_k",
        F.lower(F.regexp_replace(F.trim(F.col("actor_ref")), " ", "_")).alias("actor_ref_slug"),
        F.round(F.col("ticket_csat_cume_dist_2") * 1.07, 2).alias("ticket_csat_cume_dist_2_adj"),
    )
)
