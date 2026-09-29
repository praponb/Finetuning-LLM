from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_actor_is_not_null_ticket_i = Window.partitionBy("n_actor_is_not_null", "ticket_id").orderBy(F.col("n_actor_is_not_null").asc_nulls_last(), "ticket_id", "event_at_quarter_ym", "event_at_month")
w_n_actor_is_not_null_ticket_i_2 = Window.partitionBy("n_actor_is_not_null", "ticket_id").orderBy(F.col("n_actor_is_not_null").asc_nulls_last(), "ticket_id", "event_at_quarter_ym", "event_at_month").rowsBetween(-2, Window.currentRow)
w_ticket_id_n_actor_is_not_nul = Window.partitionBy("ticket_id", "n_actor_is_not_null").orderBy("n_actor_is_not_null", "ticket_id", "event_at_quarter_ym", "event_at_month")
w_n_actor_is_not_null = Window.partitionBy("n_actor_is_not_null")
w_ticket_id_n_actor_is_not_nul_2 = Window.partitionBy("ticket_id", "n_actor_is_not_null").orderBy("n_actor_is_not_null")
w_ticket_id_n_actor_is_not_nul_3 = Window.partitionBy("ticket_id", "n_actor_is_not_null").orderBy("n_actor_is_not_null", "ticket_id", "event_at_quarter_ym", "event_at_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-24").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("event_type"))).alias("event_type_clean"),
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean_2"),
    )
)

tagged = (
    prepared
    .select(
        "*",
        F.lower(F.trim(F.col("event_type_clean"))).alias("event_type_clean_clean"),
        F.substring(F.trim(F.col("event_type_clean_2")), 1, 3).alias("event_type_clean_2_prefix"),
    )
)

filtered = (
    tagged.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.to_date(F.col("t.event_at")) < F.lit("2025-02-06").cast("date")), "left_semi")
)

labeled = (
    filtered
    .select(
        "*",
        F.concat_ws("-", F.col("event_type_clean_clean"), F.col("event_type")).alias("event_type_clean_clean_event_type_label"),
        F.date_trunc("quarter", F.col("event_at")).alias("event_at_quarter"),
    )
)

summary = (
    labeled
    .filter(F.col("event_type") != "created")
    .groupBy(
        "ticket_id",
        F.date_format("event_at_quarter", "yyyy-MM").alias("event_at_quarter_ym"),
        F.date_trunc("month", "event_at").alias("event_at_month"),
    )
    .agg(
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.count(F.when(F.col("actor").isNotNull(), 1)).alias("n_actor_is_not_null"),
    )
)

metrics = (
    summary
    .select(
        "*",
        F.dense_rank().over(w_n_actor_is_not_null_ticket_i).alias("n_actor_is_not_null_dense_rank"),
        F.sum("n_actor_is_not_null").over(w_n_actor_is_not_null_ticket_i_2).alias("n_actor_is_not_null_sum_3"),
        F.ntile(4).over(w_ticket_id_n_actor_is_not_nul).alias("n_actor_is_not_null_quartile"),
        F.round(F.col("n_actor_is_not_null") - F.avg("n_actor_is_not_null").over(w_n_actor_is_not_null), 2).alias("n_actor_is_not_null_vs_n_actor_is_not_null_avg"),
        F.dense_rank().over(w_ticket_id_n_actor_is_not_nul_2).alias("n_actor_is_not_null_dense_rank_2"),
        F.last("event_at_month", ignorenulls=True).over(w_ticket_id_n_actor_is_not_nul_3).alias("last_known_event_at_month"),
    )
)

result = (
    metrics
    .filter(F.col("n_actor_is_not_null_sum_3") >= 4)
    .select(
        "ticket_id",
        "event_at_quarter_ym",
        "event_at_month",
        "event_type_list",
        "n_actor_is_not_null_dense_rank",
        "n_actor_is_not_null_sum_3",
        "n_actor_is_not_null_vs_n_actor_is_not_null_avg",
        "n_actor_is_not_null_dense_rank_2",
    )
    .orderBy(F.col("n_actor_is_not_null_sum_3").desc_nulls_first())
)
