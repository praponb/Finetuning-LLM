from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_event_type_in_replied_crea = Window.partitionBy("n_event_type_in_replied_create").orderBy("n_event_type_in_replied_create")
w_n_event_type_in_replied_crea_2 = Window.partitionBy("n_event_type_in_replied_create").orderBy(F.col("n_event_type_in_replied_create").desc())
w_all_by_n_event_type_in_repli = Window.orderBy(F.col("n_event_type_in_replied_create").asc_nulls_last(), "event_at_month_ym")

recent_events = (
    df_ticket_events
    .filter((F.col("actor") != "customer") & (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 30 MINUTES")).alias("event_at_plus_30m"),
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
    )
)

enriched = (
    recent_events
    .select(
        "id",
        "event_type",
        "event_at_month",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

rollup_base = (
    enriched
    .groupBy(F.date_format("event_at_month", "yyyy-MM").alias("event_at_month_ym"))
    .agg(
        F.sum(F.when(F.col("event_type").isin("resolved", "assigned"), 1).otherwise(0)).alias("n_event_type_in_resolved_assig"),
        F.count(F.when(F.col("event_type").isin("replied", "created", "reopened"), 1)).alias("n_event_type_in_replied_create"),
        F.min("event_at_month").alias("first_event_at_month"),
    )
)

screened = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("n_event_type_in_resolved_assig").alias("overall_avg_n_event_type_in_resolved_assig")))
    .filter(F.col("n_event_type_in_resolved_assig") > F.col("overall_avg_n_event_type_in_resolved_assig"))
    .drop("overall_avg_n_event_type_in_resolved_assig")
)

annotated = (
    screened
    .select(
        "event_at_month_ym",
        "n_event_type_in_replied_create",
        F.substring(F.trim(F.col("event_at_month_ym")), 1, 3).alias("event_at_month_ym_prefix"),
    )
)

metrics = (
    annotated
    .select(
        "*",
        F.dense_rank().over(w_n_event_type_in_replied_crea).alias("n_event_type_in_replied_create_dense_rank"),
        F.round(F.cume_dist().over(w_n_event_type_in_replied_crea_2), 3).alias("n_event_type_in_replied_create_cume_dist"),
        F.ntile(10).over(w_all_by_n_event_type_in_repli).alias("n_event_type_in_replied_create_decile"),
        F.dense_rank().over(w_all_by_n_event_type_in_repli).alias("n_event_type_in_replied_create_dense_rank_2"),
    )
)

without_matches = (
    metrics
    .crossJoin(metrics.agg(F.avg("n_event_type_in_replied_create_dense_rank").alias("overall_avg_n_event_type_in_replied_create_dense")))
    .filter(F.col("n_event_type_in_replied_create_dense_rank") > F.col("overall_avg_n_event_type_in_replied_create_dense"))
    .drop("overall_avg_n_event_type_in_replied_create_dense")
)

result = (
    without_matches
    .select(
        "event_at_month_ym",
        "n_event_type_in_replied_create",
        "event_at_month_ym_prefix",
        "n_event_type_in_replied_create_dense_rank",
        "n_event_type_in_replied_create_cume_dist",
        "n_event_type_in_replied_create_decile",
        "n_event_type_in_replied_create_dense_rank_2",
    )
)
