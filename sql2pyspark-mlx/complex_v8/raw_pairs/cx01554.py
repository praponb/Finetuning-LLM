from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("ticket_id").orderBy(F.col("days_since_event_at").desc(), F.col("id").desc())
w_n_rows_by_n_rows = Window.partitionBy("n_rows").orderBy("n_rows", "event_at_date_ym")
w_last_event_at_date_year_n_ro = Window.partitionBy(F.year("last_event_at_date"), "n_rows")
w_last_event_at_date_month = Window.partitionBy(F.date_trunc("month", "last_event_at_date"))
w_n_rows_tercile_by_last_event = Window.partitionBy("n_rows_tercile").orderBy("last_event_at_date", "event_at_date_ym")
w_last_event_at_date_month_by = Window.partitionBy(F.month("last_event_at_date")).orderBy(F.col("last_event_at_date_year_n_rows_avg_min_days_sinc").desc(), "event_at_date_ym").rowsBetween(-6, -1)

valid_events = (
    df_ticket_events
    .filter(F.col("event_type").isin("reopened", "resolved", "replied"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.max("event_at").alias("last_event_at"),
    )
)

summarized_rows = (
    valid_events.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.last_event_at",
    )
)

latest = (
    summarized_rows
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

stats = (
    latest
    .groupBy(F.date_format("event_at_date", "yyyy-MM").alias("event_at_date_ym"))
    .agg(
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.max("event_at_date").alias("last_event_at_date"),
        F.min("days_since_event_at").alias("min_days_since_event_at"),
    )
)

without_matches = (
    stats
    .crossJoin(stats.agg(F.avg("n_rows").alias("overall_avg_n_rows")))
    .filter(F.col("n_rows") > F.col("overall_avg_n_rows"))
    .drop("overall_avg_n_rows")
)

with_trends = (
    without_matches
    .select(
        "*",
        F.ntile(3).over(w_n_rows_by_n_rows).alias("n_rows_tercile"),
        F.round(F.avg("min_days_since_event_at").over(w_last_event_at_date_year_n_ro), 2).alias("last_event_at_date_year_n_rows_avg_min_days_sinc"),
    )
)

ranked = (
    with_trends
    .select(
        "*",
        F.round(F.col("n_rows_tercile") - F.avg("n_rows_tercile").over(w_last_event_at_date_month), 2).alias("n_rows_tercile_vs_last_event_at_date_month_avg"),
        F.rank().over(w_n_rows_tercile_by_last_event).alias("last_event_at_date_rank"),
        F.sum("n_rows").over(w_last_event_at_date_month_by).alias("n_rows_sum_6_prior"),
    )
)

result = (
    ranked
    .filter(F.col("n_rows_sum_6_prior") <= 29)
    .select(
        "event_at_date_ym",
        "n_rows",
        "actor_list",
        "last_event_at_date",
        "min_days_since_event_at",
        "n_rows_tercile",
        "last_event_at_date_year_n_rows_avg_min_days_sinc",
        "n_rows_tercile_vs_last_event_at_date_month_avg",
        "n_rows_sum_6_prior",
    )
    .orderBy(F.col("n_rows_sum_6_prior").desc(), F.col("n_rows_tercile").desc())
)
