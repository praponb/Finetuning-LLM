from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_prior_rows_3d = Window.partitionBy("agent_id").orderBy("prior_rows_3d", "id")
w_agent_id = Window.partitionBy("agent_id")

joined_ranges = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.csat").alias("prior_csat_3d"),
    )
)

sequenced = (
    joined_ranges
    .select(
        "*",
        F.round(F.percent_rank().over(w_agent_id_by_prior_rows_3d), 3).alias("prior_rows_3d_percent_rank"),
        F.min("prior_rows_3d").over(w_agent_id).alias("agent_id_min_prior_rows_3d"),
    )
)

result = (
    sequenced
    .filter(F.col("opened_at") >= F.lit("2025-01-25 11:00:00").cast("timestamp"))
    .select(
        "id",
        "opened_at",
        "prior_rows_3d",
        "prior_csat_3d",
        "prior_rows_3d_percent_rank",
        "agent_id_min_prior_rows_3d",
    )
    .orderBy("prior_rows_3d_percent_rank", "prior_rows_3d")
)
