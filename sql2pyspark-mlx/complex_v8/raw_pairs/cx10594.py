from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_at = Window.orderBy("event_at", "id")
w_all_by_event_at_full = Window.orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_ticket_id_by_event_at = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_ticket_id_by_event_at_2 = Window.partitionBy("actor", "ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_actor_ticket_id_by_event_at_3 = Window.partitionBy("actor", "ticket_id").orderBy(F.col("event_at").asc_nulls_last(), "id")

valid = (
    df_ticket_events
    .filter(F.col("actor").isin("customer", "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.substring(F.trim(F.col("event_type")), 1, 3).alias("event_type_prefix"),
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.lag("event_at").over(w_all_by_event_at).alias("prev_event_at"),
        F.nth_value("event_at", 2).over(w_all_by_event_at_full).alias("second_event_at"),
        F.nth_value("event_at", 2).over(w_actor_ticket_id_by_event_at).alias("second_event_at_2"),
        F.first("event_at", ignorenulls=True).over(w_actor_ticket_id_by_event_at_2).alias("first_known_event_at"),
        F.row_number().over(w_actor_ticket_id_by_event_at_3).alias("seq_no"),
        F.ntile(5).over(w_all_by_event_at).alias("event_at_quintile"),
    )
)

tagged = (
    windowed
    .filter(F.col("event_type_prefix").isNotNull())
    .withColumn("prev_event_at_label", F.date_format(F.col("prev_event_at"), "MMM yyyy"))
)

result = (
    tagged
    .select(
        "id",
        "event_at",
        "event_type",
        "event_type_prefix",
        "actor_clean",
        "event_at_label",
        "prev_event_at",
        "second_event_at",
        "second_event_at_2",
        "first_known_event_at",
        "seq_no",
        F.abs(F.col("event_at_quintile") - 5).alias("event_at_quintile_distance"),
        (F.col("event_at_quintile") - F.col("seq_no")).alias("event_at_quintile_minus_seq_no"),
    )
    .orderBy(F.col("actor_clean").desc(), F.col("seq_no").asc())
)
