from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_by_event_at_full = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_date_month_actor_by = Window.partitionBy(F.month("event_at_date"), "actor").orderBy(F.col("event_at_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_at_month_ticket_id_by = Window.partitionBy(F.month("event_at"), "ticket_id").orderBy("event_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_event_at_date_month_event_ty = Window.partitionBy(F.month("event_at_date"), "event_type").orderBy("event_at", "id")
w_all_by_event_at = Window.orderBy(F.col("event_at").desc_nulls_first(), "id")
w_all_by_event_at_ties = Window.orderBy(F.col("event_at").desc_nulls_first())
w_event_at_date_month_actor_by_2 = Window.partitionBy(F.month("event_at_date"), "actor").orderBy(F.col("event_at_date").desc_nulls_first(), "id")
w_ticket_id_by_event_at = Window.partitionBy("ticket_id").orderBy(F.col("event_at").desc(), "id")

base = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

with_trends = (
    base
    .select(
        "*",
        F.last("event_at").over(w_ticket_id_by_event_at_full).alias("last_event_at"),
        F.first("event_at_date", ignorenulls=True).over(w_event_at_date_month_actor_by).alias("first_known_event_at_date"),
        F.first("event_at").over(w_event_at_month_ticket_id_by).alias("first_event_at"),
        F.round((F.unix_timestamp("event_at") - F.unix_timestamp(F.lag("event_at").over(w_event_at_date_month_event_ty))) / 60, 1).alias("minutes_since_prev_event_at"),
        F.round(F.percent_rank().over(w_event_at_date_month_event_ty), 3).alias("event_at_percent_rank"),
        F.ntile(4).over(w_all_by_event_at).alias("event_at_quartile"),
        F.row_number().over(w_event_at_date_month_event_ty).alias("event_at_seq"),
        F.rank().over(w_all_by_event_at).alias("event_at_rank"),
        F.ntile(10).over(w_all_by_event_at).alias("event_at_decile"),
        F.round(F.cume_dist().over(w_all_by_event_at_ties), 3).alias("event_at_cume_dist"),
        F.ntile(4).over(w_event_at_date_month_actor_by_2).alias("event_at_date_quartile"),
        F.round(F.percent_rank().over(w_ticket_id_by_event_at), 3).alias("event_at_percent_rank_2"),
    )
)

labeled = (
    with_trends
    .select(
        "*",
        F.when(F.col("event_type").isin("assigned", "reopened"), "group_a").when(F.col("event_type") == "replied", "group_b").otherwise("other").alias("event_type_group"),
        F.substring(F.trim(F.col("event_type")), 1, 2).alias("event_type_prefix"),
    )
)

result = (
    labeled
    .select(
        "id",
        "ticket_id",
        "event_at_date",
        "first_known_event_at_date",
        "minutes_since_prev_event_at",
        "event_at_quartile",
        "event_at_rank",
        "event_at_decile",
        "event_at_cume_dist",
        "event_at_date_quartile",
        "event_at_percent_rank_2",
        "event_type_group",
        F.lower(F.regexp_replace(F.trim(F.col("event_type_prefix")), " ", "_")).alias("event_type_prefix_slug"),
    )
    .orderBy(
        F.col("event_type_prefix_slug").desc(),
        F.col("event_at_date").desc_nulls_first(),
    )
)
