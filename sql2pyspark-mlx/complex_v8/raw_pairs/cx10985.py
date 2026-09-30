from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hired_on_quarter_team_by_hir = Window.partitionBy("hired_on_quarter", "team").orderBy("hired_on_quarter", "id")
w_team_by_hired_on_plus_1m = Window.partitionBy("team").orderBy("hired_on_plus_1m", "id")
w_hired_on_quarter_team_by_hir_2 = Window.partitionBy("hired_on_quarter", "team").orderBy("hired_on_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hired_on_quarter_team_by_hir_3 = Window.partitionBy("hired_on_quarter", "team").orderBy("hired_on_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_hired_on_quarter_team_by_hir_4 = Window.partitionBy("hired_on_quarter", "team").orderBy("hired_on_quarter", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_support_agents
    .filter(F.col("team").isNotNull())
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.add_months(F.col("hired_on"), 1).alias("hired_on_plus_1m"),
        F.quarter(F.col("hired_on")).alias("hired_on_quarter"),
        F.lower(F.trim(F.col("tier"))).alias("tier_clean"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "tier",
        "hired_on_quarter",
        F.datediff(F.col("hired_on_plus_1m"), F.lag("hired_on_plus_1m").over(w_hired_on_quarter_team_by_hir)).alias("days_since_prev_hired_on_plus_1m"),
        F.rank().over(w_team_by_hired_on_plus_1m).alias("hired_on_plus_1m_rank"),
        F.lag("hired_on_plus_1m", 3).over(w_hired_on_quarter_team_by_hir).alias("prev3_hired_on_plus_1m"),
        F.max("hired_on_quarter").over(w_hired_on_quarter_team_by_hir_2).alias("max_hired_on_quarter_so_far"),
        F.first("hired_on_plus_1m").over(w_hired_on_quarter_team_by_hir_3).alias("first_hired_on_plus_1m"),
        F.round(F.avg("hired_on_quarter").over(w_hired_on_quarter_team_by_hir_4), 2).alias("hired_on_quarter_avg_3"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.max("csat").alias("max_ticket_csat"),
        F.sum("csat").alias("total_ticket_csat"),
        F.max("closed_at").alias("last_ticket_closed_at"),
    )
)

with_stats = (
    metrics.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.max_ticket_csat",
        "t.total_ticket_csat",
        "t.last_ticket_closed_at",
    )
)

first_rows = with_stats.filter(F.col("hired_on_plus_1m_rank") <= 5)

tagged = (
    first_rows
    .select(
        "*",
        F.greatest(F.col("max_ticket_csat"), F.col("hired_on_quarter_avg_3")).alias("greatest_max_ticket_csat_hired_on_quarter_avg_3"),
        F.greatest(F.col("hired_on_plus_1m_rank"), F.col("hired_on_quarter")).alias("greatest_hired_on_plus_1m_rank_hired_on_quarter"),
        F.round(F.col("hired_on_quarter_avg_3") * 1.1, 2).alias("hired_on_quarter_avg_3_adj"),
    )
)

scored = (
    tagged
    .select(
        "id",
        "tier",
        "hired_on_quarter",
        "days_since_prev_hired_on_plus_1m",
        "first_hired_on_plus_1m",
        "hired_on_quarter_avg_3",
        "total_ticket_csat",
        "last_ticket_closed_at",
        "greatest_max_ticket_csat_hired_on_quarter_avg_3",
        "hired_on_quarter_avg_3_adj",
        F.round(F.col("hired_on_quarter_avg_3") * 0.9, 2).alias("hired_on_quarter_avg_3_adj_2"),
    )
)

derived = (
    scored
    .filter(F.month(F.col("last_ticket_closed_at")).isin(2, 3))
    .select(
        "id",
        "tier",
        "days_since_prev_hired_on_plus_1m",
        "first_hired_on_plus_1m",
        "last_ticket_closed_at",
        "hired_on_quarter_avg_3_adj_2",
        (F.col("hired_on_quarter_avg_3") - F.col("total_ticket_csat")).alias("hired_on_quarter_avg_3_minus_total_ticket_csat"),
    )
)

result = (
    derived
    .select(
        "id",
        "days_since_prev_hired_on_plus_1m",
        "first_hired_on_plus_1m",
        "last_ticket_closed_at",
        "hired_on_quarter_avg_3_adj_2",
        F.upper(F.trim(F.col("tier"))).alias("tier_clean"),
        F.date_format(F.col("first_hired_on_plus_1m"), "MMM yyyy").alias("first_hired_on_plus_1m_label"),
    )
    .orderBy(F.col("tier_clean").desc())
)
