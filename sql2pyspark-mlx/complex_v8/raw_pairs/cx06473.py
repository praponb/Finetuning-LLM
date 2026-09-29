from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("team", "tier").orderBy("hired_on", "id")
w_hired_on_year_by_hired_on = Window.partitionBy(F.year("hired_on")).orderBy("hired_on", "id")
w_hired_on_year_by_hired_on_ru = Window.partitionBy(F.year("hired_on")).orderBy("hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hired_on_month = Window.partitionBy(F.date_trunc("month", "hired_on"))
w_tier_seq_no_by_hired_on_prio = Window.partitionBy("tier", "seq_no").orderBy("hired_on", "id").rowsBetween(-2, -1)

recent_agents = (
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

latest = (
    recent_agents
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

windowed = (
    latest
    .select(
        "id",
        "tier",
        "hired_on",
        F.round(F.cume_dist().over(w_hired_on_year_by_hired_on), 3).alias("hired_on_cume_dist"),
        F.row_number().over(w_hired_on_year_by_hired_on).alias("seq_no"),
        F.first("hired_on", ignorenulls=True).over(w_hired_on_year_by_hired_on_ru).alias("first_known_hired_on"),
    )
)

scored = (
    windowed
    .select(
        "*",
        F.substring(F.trim(F.col("tier")), 1, 4).alias("tier_prefix"),
        F.date_format(F.col("first_known_hired_on"), "yyyy-MM-dd").alias("first_known_hired_on_label"),
        F.concat(F.col("tier"), F.lit("#"), F.col("seq_no").cast("string")).alias("tier_ref"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.count(F.lit(1)).over(w_hired_on_month).alias("rows_in_hired_on_month"),
        F.max("seq_no").over(w_tier_seq_no_by_hired_on_prio).alias("seq_no_max_2_prior"),
        F.sum("hired_on_cume_dist").over(w_hired_on_month).alias("hired_on_month_sum_hired_on_cume_dist"),
    )
)

tier_stats = (
    with_windows
    .groupBy("tier")
    .agg(F.avg("seq_no").alias("avg_seq_no"))
)

compared = (
    with_windows.alias("s")
    .join(tier_stats.alias("g"), F.col("g.tier") == F.col("s.tier"), "inner")
    .filter(F.col("s.seq_no") > F.col("g.avg_seq_no"))
    .select(
        "s.*",
        "g.avg_seq_no",
        F.round(F.col("s.seq_no") - F.col("g.avg_seq_no"), 2).alias("seq_no_minus_avg"),
    )
)

detail_rows_tickets = (
    compared.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.id")) & (~F.col("t.priority").isin("P4", "P2")), "inner")
    .select(
        "s.id",
        "s.tier",
        "s.hired_on",
        "s.hired_on_cume_dist",
        "s.seq_no",
        "s.first_known_hired_on",
        "s.tier_prefix",
        "s.first_known_hired_on_label",
        "s.tier_ref",
        "s.avg_seq_no",
        F.col("t.id").alias("ticket_id"),
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

result = (
    detail_rows_tickets
    .filter(F.col("ticket_csat").between(2, 5))
    .select(
        "id",
        "tier",
        "hired_on",
        "hired_on_cume_dist",
        "seq_no",
        "first_known_hired_on",
        "tier_prefix",
        "first_known_hired_on_label",
        "tier_ref",
        "avg_seq_no",
        "ticket_id",
        "ticket_csat",
        "ticket_priority",
    )
    .orderBy("tier_ref", F.col("ticket_id").asc_nulls_last(), "id")
    .limit(20)
)
