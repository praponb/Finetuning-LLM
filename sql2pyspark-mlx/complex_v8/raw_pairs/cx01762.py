from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_at_month_by_event_at = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy(F.col("event_at").desc(), "id")
w_event_at_month_by_event_at_r = Window.partitionBy(F.date_trunc("month", "event_at")).orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc(), "id")
w_event_type_by_event_at_runni = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_ticket_events
    .filter((F.col("event_type") == "replied") & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

metrics = (
    filtered
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        F.ntile(5).over(w_event_at_month_by_event_at).alias("event_at_quintile"),
        F.first("event_at", ignorenulls=True).over(w_event_at_month_by_event_at_r).alias("first_known_event_at"),
        F.ntile(3).over(w_event_at_month_by_event_at).alias("event_at_tercile"),
        F.lag("event_at").over(w_ticket_id_event_type_by_even).alias("prev_event_at"),
        F.last("event_at", ignorenulls=True).over(w_event_type_by_event_at_runni).alias("last_known_event_at"),
    )
)

result = (
    metrics
    .groupingSets(
        [["event_at_tercile", "ticket_id"], ["event_at_tercile"]],
        "event_at_tercile", "ticket_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("event_at_quintile") / F.nullif(F.sum("event_at_tercile"), F.lit(0)), 3).alias("event_at_quintile_to_event_at_tercile_ratio"),
    )
    .orderBy(
        F.col("event_at_tercile").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
    )
)
