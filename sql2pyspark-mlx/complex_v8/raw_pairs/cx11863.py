from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_month_csat = Window.partitionBy(F.date_trunc("month", "opened_at"), "csat")
w_all_by_requester_id = Window.orderBy(F.col("requester_id").desc(), "id")
w_agent_tier_csat_by_requester = Window.partitionBy("agent_tier", "csat").orderBy("requester_id")

prepared = (
    df_tickets
    .filter(F.col("agent_id") <= 7)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.when(F.col("requester_id") >= 17, "HIGH").when(F.col("requester_id") >= 8, "MEDIUM").otherwise("LOW").alias("requester_id_tier"),
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
    )
)

enriched = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select("s.*", "su.agent_name", F.col("su.tier").alias("agent_tier"))
)

annotated = (
    enriched
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "requester_id_tier",
        "agent_tier",
        F.when(F.col("requester_id_filled") >= 20, "large").when(F.col("requester_id_filled") >= 11, "medium").otherwise("small").alias("requester_id_filled_tier"),
    )
)

metrics = (
    annotated
    .select(
        "*",
        F.max("requester_id").over(w_opened_at_month_csat).alias("opened_at_month_csat_max_requester_id"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_opened_at_month_csat), F.lit(0)), 4).alias("requester_id_share_of_opened_at_month_csat"),
        F.rank().over(w_all_by_requester_id).alias("requester_id_rank"),
        F.dense_rank().over(w_agent_tier_csat_by_requester).alias("requester_id_dense_rank"),
    )
)

screened = (
    metrics.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type").isin("assigned", "resolved", "escalated")), "left_anti")
)

grouped = (
    screened
    .filter(F.col("agent_id").isNotNull())
    .groupBy(
        "requester_id_tier",
        F.when(F.col("requester_id") >= 18, "A").when(F.col("requester_id") >= 7, "B").otherwise("C").alias("requester_id_tier_2"),
        F.year("opened_at").alias("opened_at_year"),
    )
    .agg(
        F.countDistinct("requester_id_tier").alias("distinct_requester_id_tiers"),
        F.round(F.stddev("csat"), 2).alias("stddev_csat"),
        F.percentile_approx("csat", 0.5).alias("median_csat"),
        F.count("*").alias("cnt"),
        F.sum("requester_id").alias("total_requester_id"),
        F.count(F.when(F.col("agent_tier").isNotNull(), 1)).alias("n_agent_tier_is_not_null"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    grouped
    .filter(F.col("requester_id_tier_2") == "A")
    .select(
        "requester_id_tier",
        "requester_id_tier_2",
        "opened_at_year",
        "distinct_requester_id_tiers",
        "stddev_csat",
        "median_csat",
        "cnt",
        "total_requester_id",
        F.round(F.col("opened_at_year") / F.nullif(F.col("n_agent_tier_is_not_null"), F.lit(0)), 3).alias("opened_at_year_per_n_agent_tier_is_not_null"),
    )
    .orderBy(F.col("requester_id_tier").desc())
)
