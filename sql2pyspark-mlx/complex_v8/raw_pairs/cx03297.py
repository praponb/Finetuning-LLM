from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_csat_per_requester_id = Window.orderBy(F.col("csat_per_requester_id").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_csat_filled_by_csat_fil = Window.partitionBy("csat", "csat_filled").orderBy(F.col("csat_filled").desc())

scoped = (
    df_tickets
    .filter((F.month(F.col("opened_at")).isin(2, 3)) & (F.col("priority").isin("P4", "P2", "P3")))
    .select(
        "id",
        "agent_id",
        "priority",
        "csat",
        "tags",
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
    )
)

without_matches = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier") == "T3") & (F.col("su.hired_on").isNotNull()), "left_semi")
)

filtered = (
    without_matches.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.team").isNotNull()), "left_anti")
)

scored = (
    filtered
    .select(
        "id",
        "agent_id",
        "csat",
        "tags",
        "csat_per_requester_id",
        F.greatest(F.col("csat_per_requester_id"), F.col("csat")).alias("greatest_csat_per_requester_id_csat"),
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
)

sequenced = (
    scored
    .select(
        "id",
        "agent_id",
        "csat",
        "tags",
        "csat_per_requester_id",
        "greatest_csat_per_requester_id_csat",
        "csat_filled",
        F.last("csat_filled").over(w_all_by_csat_per_requester_id).alias("last_csat_filled"),
        F.round(F.percent_rank().over(w_csat_csat_filled_by_csat_fil), 3).alias("csat_filled_percent_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "agent_id",
        "csat_per_requester_id",
        "greatest_csat_per_requester_id_csat",
        "csat_filled",
        "last_csat_filled",
        "csat_filled_percent_rank",
        F.coalesce(F.col("csat_per_requester_id"), F.lit(0.0)).alias("csat_per_requester_id_filled"),
    )
    .orderBy("greatest_csat_per_requester_id_csat", F.col("csat_filled").desc())
)
