from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id_event_type_by_even = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_ticket_id_event_type_by_even_2 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_event_type_by_even_3 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc(), "id")
w_ticket_id_event_type_by_even_4 = Window.partitionBy("ticket_id", "event_type").orderBy(F.col("event_at").desc(), "id").rowsBetween(-6, -1)
w_actor_event_type_by_event_co = Window.partitionBy("actor", "event_type").orderBy("event_count_sum_6_prior", "id").rowsBetween(-1, -1)
w_actor_event_type_by_event_co_2 = Window.partitionBy("actor", "event_type").orderBy("event_count_sum_6_prior", "id")
w_event_type_by_rn_prior_3 = Window.partitionBy("event_type").orderBy(F.col("rn").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_actor_event_type_by_event_co_3 = Window.partitionBy("actor", "event_type").orderBy("event_count_sum_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_type_by_rn_ties = Window.partitionBy("event_type").orderBy(F.col("rn").asc_nulls_last())

prepared = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"))
)

summarized_rows = (
    prepared.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
    )
)

metrics = (
    summarized_rows
    .select(
        "*",
        F.max("event_count").over(w_ticket_id_event_type_by_even).alias("event_count_max_7"),
        F.max("event_count").over(w_ticket_id_event_type_by_even_2).alias("max_event_count_so_far"),
        F.row_number().over(w_ticket_id_event_type_by_even_3).alias("rn"),
        F.sum("event_count").over(w_ticket_id_event_type_by_even_4).alias("event_count_sum_6_prior"),
    )
)

ranked = (
    metrics
    .select(
        "*",
        F.sum("event_count_sum_6_prior").over(w_actor_event_type_by_event_co).alias("event_count_sum_6_prior_sum_1_prior"),
        F.lag("rn").over(w_actor_event_type_by_event_co_2).alias("prev_rn"),
        F.max("event_count").over(w_event_type_by_rn_prior_3).alias("event_count_max_3_prior"),
        F.round(F.percent_rank().over(w_actor_event_type_by_event_co_2), 3).alias("event_count_sum_6_prior_percent_rank"),
        F.count(F.lit(1)).over(w_actor_event_type_by_event_co_3).alias("running_count"),
        F.round(F.cume_dist().over(w_event_type_by_rn_ties), 3).alias("rn_cume_dist"),
    )
)

result = (
    ranked
    .select(
        "id",
        "ticket_id",
        "event_count",
        "event_count_max_7",
        "max_event_count_so_far",
        "event_count_sum_6_prior",
        "event_count_sum_6_prior_sum_1_prior",
        "prev_rn",
        "running_count",
        "rn_cume_dist",
        F.when(F.col("rn_cume_dist") >= 0.8, "HIGH").when(F.col("rn_cume_dist") >= 0.5, "MEDIUM").otherwise("LOW").alias("rn_cume_dist_tier"),
        F.greatest(F.col("max_event_count_so_far"), F.col("event_count")).alias("greatest_max_event_count_so_far_event_count"),
    )
    .orderBy(F.col("event_count_sum_6_prior").desc())
)
