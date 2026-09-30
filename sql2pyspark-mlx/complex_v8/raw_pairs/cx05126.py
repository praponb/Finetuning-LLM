from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("actor", "event_type").orderBy(F.col("event_at_year").desc(), F.col("id").desc())

filtered_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label_2"),
    )
)

annotated = filtered_events.withColumn("event_at_year", F.year(F.col("event_at")))

actor_stats = (
    annotated
    .groupBy("actor")
    .agg(F.avg("event_at_year").alias("avg_event_at_year"))
)

above_average = (
    annotated.alias("s")
    .join(actor_stats.alias("g"), F.col("g.actor") == F.col("s.actor"), "inner")
    .filter(F.col("s.event_at_year") > F.col("g.avg_event_at_year"))
    .select(
        "s.*",
        "g.avg_event_at_year",
        F.round(F.col("s.event_at_year") - F.col("g.avg_event_at_year"), 2).alias("event_at_year_minus_avg"),
    )
)

deduped = (
    above_average
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .groupingSets(
        [["actor", "event_at_year"], ["actor"], []],
        "actor", "event_at_year",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.stddev("avg_event_at_year"), 2).alias("stddev_avg_event_at_year"),
    )
    .orderBy(
        F.col("actor").asc_nulls_last(),
        F.col("event_at_year").asc_nulls_last(),
    )
)
