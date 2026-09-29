from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("event_type").orderBy("event_at_dayofweek", "id")

scoped_events = (
    df_ticket_events
    .filter((F.col("event_type").isNotNull()) & (F.col("actor") == "system"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.quarter(F.col("event_at")).alias("event_at_quarter"),
    )
)

latest = (
    scoped_events
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

screened = (
    latest.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left_semi")
)

qualified = (
    screened.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") != "created") & (F.col("t.event_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")), "left_semi")
)

result = (
    qualified
    .cube("event_at_quarter", "actor")
    .agg(
        F.percentile_approx("event_at_quarter", 0.5).alias("median_event_at_quarter"),
        F.round(F.avg("event_at_quarter"), 2).alias("avg_event_at_quarter"),
        F.count(F.when(F.col("event_type") != "reopened", 1)).alias("n_event_type_reopened"),
    )
    .select(
        "event_at_quarter",
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        "median_event_at_quarter",
        "avg_event_at_quarter",
        "n_event_type_reopened",
    )
    .orderBy(
        F.col("event_at_quarter").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
    )
)
