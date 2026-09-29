from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("ticket_id").orderBy(F.col("event_at_date").desc(), F.col("id").desc())

scoped = (
    df_ticket_events
    .filter(F.col("event_type").isin("created", "reopened", "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
)

unique_rows = (
    scoped
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

per_group = (
    unique_rows
    .groupBy(F.to_date("event_at").alias("event_at_day"))
    .agg(
        F.count(F.when(F.col("event_type") == "assigned", 1)).alias("n_event_type_assigned"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

without_matches = (
    per_group
    .crossJoin(per_group.agg(F.min("n_event_type_assigned").alias("overall_min_n_event_type_assigned")))
    .filter(F.col("n_event_type_assigned") > F.col("overall_min_n_event_type_assigned"))
    .drop("overall_min_n_event_type_assigned")
)

annotated = (
    without_matches
    .filter(F.col("cnt") < 8)
    .select("*", (F.floor(F.col("cnt") / 2) * 2).alias("cnt_bucket"))
)

result = (
    annotated
    .select(
        "event_at_day",
        "n_event_type_assigned",
        "cnt",
        "cnt_bucket",
        F.last_day(F.col("event_at_day")).alias("event_at_day_month_end"),
        F.date_trunc("month", F.col("event_at_day")).alias("event_at_day_month"),
    )
    .orderBy(
        F.col("cnt").asc_nulls_last(),
        F.col("n_event_type_assigned").desc_nulls_first(),
    )
)
