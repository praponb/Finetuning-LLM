from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type_ticket_id_by_even = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").desc(), "id")
w_event_type_ticket_id_by_even_2 = Window.partitionBy("event_type", "ticket_id").orderBy(F.col("event_at").desc())
w_all_by_event_at = Window.orderBy(F.col("event_at").desc(), "id")
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy("event_at", "id")
w_all_by_event_at_full = Window.orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_event_at_decile = Window.orderBy(F.col("event_at_decile").desc(), "id")
w_event_at_decile = Window.partitionBy("event_at_decile")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc())
w_last_event_at_year = Window.partitionBy(F.year("last_event_at"))
w_event_at_decile_event_at_ter = Window.partitionBy("event_at_decile", "event_at_tercile").orderBy(F.col("event_at_quartile").asc_nulls_last(), "id")

cleaned = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-24").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        F.ntile(3).over(w_event_type_ticket_id_by_even).alias("event_at_tercile"),
        F.rank().over(w_event_type_ticket_id_by_even_2).alias("event_at_rank"),
        F.ntile(4).over(w_all_by_event_at).alias("event_at_quartile"),
        F.ntile(10).over(w_event_type_ticket_id_by_even).alias("event_at_decile"),
        F.round(F.percent_rank().over(w_event_type_ticket_id_by_even_2), 3).alias("event_at_percent_rank"),
        F.round(F.percent_rank().over(w_event_type_ticket_id_by_even), 3).alias("event_at_percent_rank_2"),
        F.lag("event_at").over(w_ticket_id_event_type_by_even).alias("prev_event_at"),
        F.last("event_at").over(w_all_by_event_at_full).alias("last_event_at"),
    )
)

leaders = with_windows.filter(F.col("event_at_rank") == 1)

with_windows_2 = (
    leaders
    .select(
        "*",
        F.row_number().over(w_all_by_event_at_decile).alias("event_at_decile_seq"),
        F.min("event_at_percent_rank_2").over(w_event_at_decile).alias("event_at_decile_min_event_at_percent_rank_2"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.round(F.avg("event_at_decile").over(w_event_at_decile), 2).alias("event_at_decile_avg_event_at_decile"),
        F.round(F.col("event_at_percent_rank_2") - F.avg("event_at_percent_rank_2").over(w_last_event_at_year), 2).alias("event_at_percent_rank_2_vs_last_event_at_year_av"),
        F.size(F.collect_set("actor").over(w_event_at_decile)).alias("distinct_actor_in_event_at_decile"),
        (F.col("event_at_tercile") - F.lag("event_at_tercile").over(w_event_at_decile_event_at_ter)).alias("event_at_tercile_change"),
    )
)

result = (
    with_windows_2
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_at_tercile",
        "event_at_quartile",
        "event_at_decile",
        "event_at_percent_rank_2",
        "prev_event_at",
        "last_event_at",
        "event_at_decile_avg_event_at_decile",
        "distinct_actor_in_event_at_decile",
        "event_at_tercile_change",
        F.round(F.col("event_at_percent_rank_2") * 1.1, 2).alias("event_at_percent_rank_2_adj"),
        F.when(F.col("event_at_decile") >= 8, "large").when(F.col("event_at_decile") >= 3, "medium").otherwise("small").alias("event_at_decile_tier"),
    )
    .orderBy(F.col("ticket_id").desc_nulls_first(), F.col("event_at_tercile").asc())
)
