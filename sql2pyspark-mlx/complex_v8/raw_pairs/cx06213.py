from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_month = Window.partitionBy(F.month("closed_at"))
w_all_by_opened_at = Window.orderBy("opened_at", "id")
w_csat_by_agent_hired_on = Window.partitionBy("csat").orderBy(F.col("agent_hired_on").desc(), "id")
w_all_by_agent_hired_on = Window.orderBy(F.col("agent_hired_on").desc(), "id")
w_closed_at_to_date = Window.partitionBy(F.to_date("closed_at"))

base = (
    df_tickets
    .filter((F.col("closed_at") >= F.lit("2025-01-12 08:00:00").cast("timestamp")) & (F.hour(F.col("opened_at")).between(8, 12)))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "tags",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

combined_details_agent = (
    base.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team") == "Onboarding"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.csat",
        "s.tags",
        "s.priority_or_unknown",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

windowed = (
    combined_details_agent
    .select(
        "*",
        F.max("csat").over(w_closed_at_month).alias("closed_at_month_max_csat"),
        (F.col("csat") - F.lag("csat").over(w_all_by_opened_at)).alias("csat_change"),
        F.round(F.percent_rank().over(w_csat_by_agent_hired_on), 3).alias("agent_hired_on_percent_rank"),
        F.lead("agent_hired_on").over(w_all_by_agent_hired_on).alias("next_agent_hired_on"),
        F.row_number().over(w_all_by_opened_at).alias("opened_at_seq"),
        F.round(F.avg("csat").over(w_closed_at_month), 2).alias("closed_at_month_avg_csat"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_closed_at_to_date), F.lit(0)), 4).alias("csat_share_of_closed_at_to_date"),
        F.sum("csat").over(w_closed_at_to_date).alias("closed_at_to_date_sum_csat"),
    )
)

first_rows = (
    windowed
    .filter((F.col("opened_at_seq") <= 2) & (F.col("closed_at_month_max_csat") >= 23))
    .drop("opened_at_seq")
)

result = (
    first_rows
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "tags",
        "priority_or_unknown",
        "agent_hired_on",
        "closed_at_month_max_csat",
        "csat_change",
        "next_agent_hired_on",
        "closed_at_month_avg_csat",
        "csat_share_of_closed_at_to_date",
        "closed_at_to_date_sum_csat",
        F.initcap(F.trim(F.col("priority_or_unknown"))).alias("priority_or_unknown_clean"),
    )
    .orderBy("csat", "id")
    .limit(10)
)
