from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")
w_actor_event_type_by_event_at = Window.partitionBy("actor", "event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_event_at_ties = Window.orderBy("event_at")
w_actor_by_event_at = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last(), "id")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy(F.col("event_at").desc(), "id")
w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy(F.col("event_at").asc_nulls_last())
w_all_by_event_at_cume_dist = Window.orderBy("event_at_cume_dist", "id")
w_next2_event_at_month_event_a = Window.partitionBy(F.date_trunc("month", "next2_event_at"), F.to_date("event_at"))
w_all_by_event_at_cume_dist_2 = Window.orderBy(F.col("event_at_cume_dist").desc_nulls_first(), "id")

scoped_events = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

ranked = (
    scoped_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "event_type_clean",
        F.lead("event_at", 2).over(w_all_by_event_at).alias("next2_event_at"),
        F.last("event_at", ignorenulls=True).over(w_actor_event_type_by_event_at).alias("last_known_event_at"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.dense_rank().over(w_all_by_event_at).alias("event_at_dense_rank"),
        F.ntile(3).over(w_all_by_event_at).alias("event_at_tercile"),
        F.lead("event_at", 2).over(w_actor_by_event_at).alias("next2_event_at_2"),
        F.round(F.cume_dist().over(w_all_by_event_at), 3).alias("event_at_cume_dist_2"),
        F.ntile(5).over(w_all_by_event_at).alias("event_at_quintile"),
        F.first("event_at").over(w_all_by_event_at_full).alias("first_event_at"),
        F.lead("event_at").over(w_event_type_by_event_at).alias("next_event_at"),
        F.round(F.cume_dist().over(w_actor_by_event_at_ties), 3).alias("event_at_cume_dist_3"),
    )
)

annotated = (
    ranked
    .filter(F.col("event_at_quintile").between(4, 9))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type_clean",
        "next2_event_at",
        "event_at_cume_dist",
        "event_at_cume_dist_2",
        F.concat_ws(": ", F.col("event_type"), F.col("event_type_clean")).alias("event_type_event_type_clean_label"),
        F.date_trunc("month", F.col("next2_event_at")).alias("next2_event_at_month"),
    )
)

with_windows = (
    annotated
    .select(
        "*",
        F.row_number().over(w_all_by_event_at_cume_dist).alias("rn"),
        F.round(F.col("event_at_cume_dist") / F.nullif(F.sum("event_at_cume_dist").over(w_next2_event_at_month_event_a), F.lit(0)), 4).alias("event_at_cume_dist_share_of_next2_event_at_month"),
        F.row_number().over(w_all_by_event_at_cume_dist_2).alias("event_at_cume_dist_seq"),
        F.round(F.col("event_at_cume_dist_2") - F.avg("event_at_cume_dist_2").over(w_next2_event_at_month_event_a), 2).alias("event_at_cume_dist_2_vs_next2_event_at_month_eve"),
        F.size(F.collect_set("ticket_id").over(w_next2_event_at_month_event_a)).alias("distinct_ticket_id_in_next2_event_at_month_event"),
        F.lag("event_at_cume_dist", 3).over(w_all_by_event_at_cume_dist_2).alias("prev3_event_at_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type_clean",
        "next2_event_at",
        "event_at_cume_dist_2",
        "next2_event_at_month",
        "rn",
        "event_at_cume_dist_share_of_next2_event_at_month",
        "event_at_cume_dist_2_vs_next2_event_at_month_eve",
        "distinct_ticket_id_in_next2_event_at_month_event",
        F.date_trunc("week", F.col("event_at")).alias("event_at_week"),
    )
    .orderBy("event_at_week", "id")
    .limit(5)
)
