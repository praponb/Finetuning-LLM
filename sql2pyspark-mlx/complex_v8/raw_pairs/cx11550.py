from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_first_first_ticket_cl = Window.orderBy("first_first_ticket_closed_at", "tier")
w_first_first_ticket_closed_at = Window.partitionBy(F.date_trunc("month", "first_first_ticket_closed_at")).orderBy(F.col("first_first_ticket_closed_at").desc(), "tier")
w_first_first_ticket_closed_at_2 = Window.partitionBy("first_first_ticket_closed_at_quintile", "tier").orderBy("first_first_ticket_closed_at", "tier")
w_first_first_ticket_closed_at_3 = Window.partitionBy(F.year("first_first_ticket_closed_at"), "n_tier_is_not_null").orderBy("prev_hired_on_span_days", "tier").rowsBetween(-1, -1)
w_tier_first_first_ticket_clos = Window.partitionBy("tier", "first_first_ticket_closed_at_quintile").orderBy(F.col("hired_on_span_days").desc(), "tier").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_first_first_ticket_closed_at_4 = Window.partitionBy("first_first_ticket_closed_at_quintile")

base = (
    df_support_agents
    .filter(F.col("team") == "Onboarding")
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.min("closed_at").alias("first_ticket_closed_at"))
)

with_counts = (
    base.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select("s.*", "t.first_ticket_closed_at")
)

totals = (
    with_counts
    .groupBy("tier")
    .agg(
        F.min("first_ticket_closed_at").alias("first_first_ticket_closed_at"),
        F.datediff(F.max("hired_on"), F.min("hired_on")).alias("hired_on_span_days"),
        F.countDistinct("tier").alias("distinct_tiers"),
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
        F.sum(F.when(F.col("tier").isNotNull(), 1).otherwise(0)).alias("n_tier_is_not_null"),
    )
)

windowed = (
    totals
    .select(
        "*",
        F.lag("hired_on_span_days").over(w_all_by_first_first_ticket_cl).alias("prev_hired_on_span_days"),
        F.ntile(5).over(w_all_by_first_first_ticket_cl).alias("first_first_ticket_closed_at_quintile"),
        F.round(F.cume_dist().over(w_first_first_ticket_closed_at), 3).alias("first_first_ticket_closed_at_cume_dist"),
    )
)

sequenced = (
    windowed
    .select(
        "tier",
        "first_first_ticket_closed_at",
        "hired_on_span_days",
        "distinct_tiers",
        "agent_name_list",
        "n_tier_is_not_null",
        "first_first_ticket_closed_at_quintile",
        "first_first_ticket_closed_at_cume_dist",
        F.row_number().over(w_first_first_ticket_closed_at_2).alias("first_first_ticket_closed_at_seq"),
        F.dense_rank().over(w_first_first_ticket_closed_at_2).alias("first_first_ticket_closed_at_dense_rank"),
        F.max("distinct_tiers").over(w_first_first_ticket_closed_at_3).alias("distinct_tiers_max_1_prior"),
        F.last("n_tier_is_not_null", ignorenulls=True).over(w_tier_first_first_ticket_clos).alias("last_known_n_tier_is_not_null"),
        F.round(F.col("distinct_tiers") - F.avg("distinct_tiers").over(w_first_first_ticket_closed_at_4), 2).alias("distinct_tiers_vs_first_first_ticket_closed_at_q"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["first_first_ticket_closed_at_dense_rank", "first_first_ticket_closed_at_seq", "distinct_tiers"], ["first_first_ticket_closed_at_dense_rank"], ["first_first_ticket_closed_at_seq"], []],
        "first_first_ticket_closed_at_dense_rank", "first_first_ticket_closed_at_seq", "distinct_tiers",
    )
    .agg(
        F.grouping("first_first_ticket_closed_at_dense_rank").alias("is_total_first_first_ticket_closed_at_dense_rank"),
        F.round(F.sum("distinct_tiers") / F.nullif(F.sum("first_first_ticket_closed_at_cume_dist"), F.lit(0)), 3).alias("distinct_tiers_to_first_first_ticket_closed_at_c"),
        F.min("hired_on_span_days").alias("min_hired_on_span_days"),
        F.sum("distinct_tiers").alias("total_distinct_tiers"),
    )
    .orderBy(
        F.col("first_first_ticket_closed_at_dense_rank").asc_nulls_last(),
        F.col("first_first_ticket_closed_at_seq").asc_nulls_last(),
        F.col("distinct_tiers").asc_nulls_last(),
    )
)
