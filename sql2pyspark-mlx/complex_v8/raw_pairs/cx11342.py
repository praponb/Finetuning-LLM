from pyspark.sql import functions as F

recent = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
    )
)

totals = (
    recent
    .groupBy(F.year("event_at").alias("event_at_year"))
    .agg(
        F.count(F.when(~F.col("event_type").isin("assigned", "reopened"), 1)).alias("n_event_type_not_in_assigned_r"),
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.min("event_at").alias("first_event_at"),
    )
    .filter(F.col("cnt") >= 2)
)

filtered = (
    totals
    .crossJoin(totals.agg(F.avg("cnt").alias("overall_avg_cnt")))
    .filter(F.col("cnt") > F.col("overall_avg_cnt"))
    .drop("overall_avg_cnt")
)

result = (
    filtered
    .select(
        "event_at_year",
        "n_event_type_not_in_assigned_r",
        "cnt",
        "event_type_list",
        F.size(F.col("event_type_list")).alias("event_type_list_count"),
    )
    .orderBy("event_at_year")
)
