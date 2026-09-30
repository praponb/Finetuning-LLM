from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_prior_rows_1d_by_pr = Window.partitionBy("agent_id", "prior_rows_1d").orderBy("prior_csat_1d", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_prior_rows_1d = Window.partitionBy("prior_rows_1d")

in_window = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.csat").alias("prior_csat_1d"),
    )
)

with_windows = (
    in_window
    .select(
        "*",
        F.nth_value("prior_csat_1d", 2).over(w_agent_id_prior_rows_1d_by_pr).alias("second_prior_csat_1d"),
        F.round(F.col("prior_csat_1d") / F.nullif(F.sum("prior_csat_1d").over(w_prior_rows_1d), F.lit(0)), 4).alias("prior_csat_1d_share_of_prior_rows_1d"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "agent_id",
        "prior_rows_1d",
        "prior_csat_1d",
        "prior_csat_1d_share_of_prior_rows_1d",
    )
    .orderBy(F.col("prior_rows_1d").desc())
)
