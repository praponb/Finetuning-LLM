from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_to_date_by_opened = Window.partitionBy(F.to_date("closed_at")).orderBy("opened_at", "id")
w_agent_team_agent_id_by_close = Window.partitionBy("agent_team", "agent_id").orderBy("closed_at", "id")
w_agent_id_agent_tier_by_opene = Window.partitionBy("agent_id", "agent_tier").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_opened_at_ties = Window.orderBy(F.col("opened_at").desc())
w_agent_id_by_closed_at_full = Window.partitionBy("agent_id").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_closed_at_to_date_by_opened_2 = Window.partitionBy(F.to_date("closed_at")).orderBy("opened_at")
w_agent_team_agent_id_by_close_2 = Window.partitionBy("agent_team", "agent_id").orderBy("closed_at")

prepared = (
    df_tickets
    .filter(F.hour(F.col("closed_at")).between(6, 8))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "tags",
    )
)

enriched_agent = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.col("su.team") == "Billing")
    .select(
        "s.*",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

sequenced = (
    enriched_agent
    .select(
        "*",
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_closed_at_to_date_by_opened))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.ntile(5).over(w_closed_at_to_date_by_opened).alias("opened_at_quintile"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_agent_team_agent_id_by_close))) / 60, 1).alias("minutes_since_prev_opened_at_2"),
        F.last("closed_at", ignorenulls=True).over(w_agent_id_agent_tier_by_opene).alias("last_known_closed_at"),
        F.rank().over(w_all_by_opened_at_ties).alias("opened_at_rank"),
        F.nth_value("closed_at", 2).over(w_agent_id_by_closed_at_full).alias("second_closed_at"),
        F.round(F.cume_dist().over(w_closed_at_to_date_by_opened_2), 3).alias("opened_at_cume_dist"),
        F.round(F.percent_rank().over(w_agent_team_agent_id_by_close), 3).alias("closed_at_percent_rank"),
        F.dense_rank().over(w_agent_team_agent_id_by_close_2).alias("closed_at_dense_rank"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_agent_team_agent_id_by_close))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.ntile(3).over(w_agent_team_agent_id_by_close).alias("closed_at_tercile"),
        F.ntile(3).over(w_closed_at_to_date_by_opened).alias("opened_at_tercile"),
    )
)

first_rows = (
    sequenced
    .filter(F.col("closed_at_dense_rank") <= 5)
    .drop("closed_at_dense_rank")
)

result = (
    first_rows
    .select(
        "id",
        "tags",
        "agent_tier",
        "opened_at_quintile",
        "minutes_since_prev_opened_at_2",
        "last_known_closed_at",
        "opened_at_rank",
        "second_closed_at",
        "opened_at_cume_dist",
        "closed_at_percent_rank",
        "opened_at_tercile",
        F.initcap(F.trim(F.col("agent_team"))).alias("agent_team_clean"),
    )
    .orderBy(
        F.col("minutes_since_prev_opened_at_2").desc(),
        F.col("opened_at_cume_dist").asc(),
    )
)
