from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_hired_on = Window.orderBy("hired_on", "id")
w_all_by_hired_on_ties = Window.orderBy("hired_on")
w_dedup = Window.partitionBy("tier").orderBy(F.col("hired_on_percent_rank_2").desc(), F.col("id").desc())

scoped_agents = (
    df_support_agents
    .filter((F.col("tier").isin("T2", "T3")) & (F.lower(F.col("agent_name")).like("%iz%")))
)

windowed = (
    scoped_agents
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on",
        F.round(F.percent_rank().over(w_all_by_hired_on), 3).alias("hired_on_percent_rank"),
        F.row_number().over(w_all_by_hired_on).alias("hired_on_seq"),
        F.dense_rank().over(w_all_by_hired_on_ties).alias("hired_on_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_hired_on), 3).alias("hired_on_cume_dist"),
        F.dense_rank().over(w_all_by_hired_on).alias("hired_on_dense_rank_2"),
        F.round(F.percent_rank().over(w_all_by_hired_on_ties), 3).alias("hired_on_percent_rank_2"),
    )
)

tier_stats = (
    windowed
    .groupBy("tier")
    .agg(F.percentile_approx("hired_on_dense_rank", 0.5).alias("median_hired_on_dense_rank"))
)

above_average = (
    windowed.alias("s")
    .join(tier_stats.alias("g"), F.col("g.tier") == F.col("s.tier"), "inner")
    .filter(F.col("s.hired_on_dense_rank") >= F.col("g.median_hired_on_dense_rank"))
    .select(
        "s.*",
        "g.median_hired_on_dense_rank",
        F.round(F.col("s.hired_on_dense_rank") - F.col("g.median_hired_on_dense_rank"), 2).alias("hired_on_dense_rank_minus_median"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count("*").alias("ticket_count"),
        F.max("opened_at").alias("last_ticket_opened_at"),
        F.min("opened_at").alias("first_ticket_opened_at"),
    )
)

summarized_rows = (
    above_average.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
        "t.last_ticket_opened_at",
        "t.first_ticket_opened_at",
    )
)

deduped = (
    summarized_rows
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

scored = (
    deduped
    .filter(F.col("ticket_count") > 2)
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on",
        "hired_on_dense_rank",
        "hired_on_cume_dist",
        "hired_on_dense_rank_2",
        "median_hired_on_dense_rank",
        "hired_on_dense_rank_minus_median",
        "ticket_count",
        "last_ticket_opened_at",
        "first_ticket_opened_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("first_ticket_opened_at")).alias("days_since_first_ticket_opened_at"),
        F.substring(F.trim(F.col("agent_name")), 1, 2).alias("agent_name_prefix"),
        F.round((F.unix_timestamp(F.col("last_ticket_opened_at")) - F.unix_timestamp(F.col("first_ticket_opened_at"))) / 60, 1).alias("minutes_first_ticket_opened_at_to_last_ticket_op"),
    )
)

result = (
    scored
    .filter(F.col("hired_on_dense_rank_2") < 7)
    .select(
        "id",
        "agent_name",
        "hired_on",
        "hired_on_dense_rank",
        "hired_on_cume_dist",
        "hired_on_dense_rank_2",
        "median_hired_on_dense_rank",
        "ticket_count",
        "last_ticket_opened_at",
        "first_ticket_opened_at",
        "days_since_first_ticket_opened_at",
        "minutes_first_ticket_opened_at_to_last_ticket_op",
    )
    .orderBy("days_since_first_ticket_opened_at", "hired_on_cume_dist")
)
