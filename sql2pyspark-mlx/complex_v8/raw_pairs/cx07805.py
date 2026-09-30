from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_event_type_len = Window.orderBy("event_type_len", "id")
w_event_type = Window.partitionBy("event_type")
w_all_by_event_type_len_moving = Window.orderBy("event_type_len", "id").rowsBetween(-2, Window.currentRow)
w_event_type_len_by_event_at_r = Window.partitionBy("event_type_len").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_len_by_event_at = Window.partitionBy("event_type_len").orderBy("event_at", "id")
w_event_type_by_event_at = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_by_event_at_prior = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(-3, -1)
w_event_type_len = Window.partitionBy("event_type_len")

base_events = (
    df_ticket_events
    .filter((F.col("event_at") >= F.lit("2025-01-25 08:00:00").cast("timestamp")) & (F.col("event_type") == "created"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

with_trends = (
    base_events
    .select(
        "*",
        F.ntile(10).over(w_all_by_event_type_len).alias("event_type_len_decile"),
        F.round(F.col("event_type_len") / F.nullif(F.sum("event_type_len").over(w_event_type), F.lit(0)), 4).alias("event_type_len_share_of_event_type"),
        F.min("event_type_len").over(w_event_type).alias("event_type_min_event_type_len"),
        F.round(F.avg("event_type_len").over(w_all_by_event_type_len_moving), 2).alias("event_type_len_avg_3"),
        F.round(F.avg("event_type_len").over(w_event_type), 2).alias("event_type_avg_event_type_len"),
        F.count(F.lit(1)).over(w_event_type).alias("rows_in_event_type"),
        F.max("event_type_len").over(w_event_type).alias("event_type_max_event_type_len"),
        F.max("event_type_len").over(w_event_type_len_by_event_at_r).alias("max_event_type_len_so_far"),
        F.rank().over(w_event_type_len_by_event_at).alias("event_at_rank"),
    )
)

annotated = (
    with_trends
    .filter(F.col("event_type_len_share_of_event_type") < 0.7)
    .withColumn("event_type_actor_label", F.concat_ws("-", F.col("event_type"), F.col("actor")))
)

metrics = (
    annotated
    .select(
        "*",
        F.lead("rows_in_event_type").over(w_event_type_by_event_at).alias("next_rows_in_event_type"),
        F.round(F.avg("event_type_len").over(w_event_type_by_event_at_prior), 2).alias("event_type_len_avg_3_prior"),
        F.round(F.col("event_type_len_avg_3") - F.avg("event_type_len_avg_3").over(w_event_type_len), 2).alias("event_type_len_avg_3_vs_event_type_len_avg"),
        F.min("event_type_len").over(w_event_type_len).alias("event_type_len_min_event_type_len"),
        F.count(F.lit(1)).over(w_event_type_len).alias("rows_in_event_type_len"),
        F.size(F.collect_set("ticket_id").over(w_event_type_len)).alias("distinct_ticket_id_in_event_type_len"),
    )
)

totals = (
    metrics
    .filter(F.col("event_type_len_avg_3_prior") < 12.2)
    .groupBy(
        F.date_trunc("month", "event_at").alias("event_at_month"),
        "event_at_rank",
    )
    .agg(
        F.sum("event_type_len_share_of_event_type").alias("total_event_type_len_share_of_event_type"),
        F.round(F.sum("event_type_len_avg_3_prior") / F.nullif(F.sum("event_type_len_avg_3"), F.lit(0)), 3).alias("event_type_len_avg_3_prior_to_event_type_len_avg"),
        F.count("*").alias("cnt"),
    )
)

result = (
    totals
    .filter(F.hour(F.col("event_at_month")).between(8, 11))
    .select(
        "event_at_month",
        "event_at_rank",
        "event_type_len_avg_3_prior_to_event_type_len_avg",
        "cnt",
    )
    .orderBy(F.col("event_at_rank").desc())
)
