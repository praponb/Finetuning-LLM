from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_actor_is_not_null_by_n_act = Window.partitionBy("n_actor_is_not_null").orderBy("n_actor_is_not_null", "actor", "event_at_month")
w_n_actor_is_not_null = Window.partitionBy("n_actor_is_not_null")
w_actor_by_pct_event_type_in_r = Window.partitionBy("actor").orderBy("pct_event_type_in_resolved_assig", "actor", "event_at_month")
w_actor_by_pct_event_type_in_r_2 = Window.partitionBy("actor").orderBy("pct_event_type_in_resolved_assig", "actor", "event_at_month").rowsBetween(-2, -1)
w_actor = Window.partitionBy("actor")
w_distinct_actor_in_n_actor_is = Window.partitionBy("distinct_actor_in_n_actor_is_not_null", "n_actor_is_not_null").orderBy(F.col("last_event_at").asc_nulls_last(), "actor", "event_at_month").rowsBetween(-1, -1)

prepared_events = (
    df_ticket_events
    .filter((F.col("event_type").isin("resolved", "replied")) & (F.col("actor") == "agent"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

stats = (
    prepared_events
    .groupBy("actor", F.date_trunc("month", "event_at").alias("event_at_month"))
    .agg(
        F.array_sort(F.collect_set("actor")).alias("actor_list"),
        F.round(100 * F.count(F.when(F.col("event_type").isin("resolved", "assigned", "created"), 1)) / F.count("*"), 1).alias("pct_event_type_in_resolved_assig"),
        F.max("event_at").alias("last_event_at"),
        F.count(F.when(F.col("actor").isNotNull(), 1)).alias("n_actor_is_not_null"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
    )
)

ranked = (
    stats
    .select(
        "*",
        F.lead("n_actor_is_not_null", 3).over(w_n_actor_is_not_null_by_n_act).alias("next3_n_actor_is_not_null"),
        F.count(F.lit(1)).over(w_n_actor_is_not_null).alias("rows_in_n_actor_is_not_null"),
        F.round((F.unix_timestamp("event_at_month") - F.unix_timestamp(F.lag("event_at_month").over(w_actor_by_pct_event_type_in_r))) / 60, 1).alias("minutes_since_prev_event_at_month"),
        F.size(F.collect_set("actor").over(w_n_actor_is_not_null)).alias("distinct_actor_in_n_actor_is_not_null"),
        F.round(F.col("n_actor_is_not_null") - F.avg("n_actor_is_not_null").over(w_n_actor_is_not_null), 2).alias("n_actor_is_not_null_vs_n_actor_is_not_null_avg"),
        F.sum("n_actor_is_not_null").over(w_actor_by_pct_event_type_in_r_2).alias("n_actor_is_not_null_sum_2_prior"),
    )
)

ranked_2 = (
    ranked
    .select(
        "*",
        F.sum("rows_in_n_actor_is_not_null").over(w_distinct_actor_in_n_actor_is).alias("rows_in_n_actor_is_not_null_sum_1_prior"),
        F.sum("next3_n_actor_is_not_null").over(w_actor).alias("actor_sum_next3_n_actor_is_not_null"),
        F.count(F.lit(1)).over(w_actor).alias("rows_in_actor"),
    )
)

result = (
    ranked_2
    .filter(F.col("actor") == "customer")
    .select(
        "actor",
        "event_at_month",
        "actor_list",
        "pct_event_type_in_resolved_assig",
        "last_event_at",
        "event_type_list",
        "next3_n_actor_is_not_null",
        "rows_in_n_actor_is_not_null",
        "minutes_since_prev_event_at_month",
        "distinct_actor_in_n_actor_is_not_null",
        "rows_in_n_actor_is_not_null_sum_1_prior",
        "actor_sum_next3_n_actor_is_not_null",
        "rows_in_actor",
    )
    .orderBy(
        F.col("actor_sum_next3_n_actor_is_not_null").desc(),
        "actor",
        "event_at_month",
    )
    .limit(10)
)
