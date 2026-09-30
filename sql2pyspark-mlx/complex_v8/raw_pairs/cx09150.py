from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor_by_event_at_full = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_actor_by_event_at_ties = Window.partitionBy("actor").orderBy("event_at")
w_actor_by_event_at = Window.partitionBy("actor").orderBy("event_at", "id")
w_dedup = Window.partitionBy("event_type").orderBy(F.col("event_at_rank").desc(), F.col("id").desc())

valid = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_type_ref"),
        F.when(F.col("actor").isin("customer", "system"), "group_a").when(F.col("actor") == "agent", "group_b").otherwise("other").alias("actor_group"),
    )
)

windowed = (
    valid
    .select(
        "*",
        F.nth_value("event_at", 2).over(w_actor_by_event_at_full).alias("second_event_at"),
        F.rank().over(w_actor_by_event_at_ties).alias("event_at_rank"),
        F.ntile(10).over(w_actor_by_event_at).alias("event_at_decile"),
    )
)

derived = (
    windowed
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "*",
        F.date_format(F.col("second_event_at"), "yyyy-MM-dd").alias("second_event_at_label"),
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
    )
)

deduped = (
    derived
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .select(
        "id",
        F.col("event_at_decile").alias("event_at_decile_v"),
        F.col("event_at_rank").alias("event_at_rank_v"),
    )
    .unpivot(
        ids=["id"],
        values=["event_at_decile_v", "event_at_rank_v"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "metric")
)
