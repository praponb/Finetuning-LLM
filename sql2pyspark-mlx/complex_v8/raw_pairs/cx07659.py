from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id")
w_opened_at_month_agent_id_by = Window.partitionBy(F.date_trunc("month", "opened_at"), "agent_id").orderBy("prior_csat_12h", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_opened_at_month_agent_id_by_2 = Window.partitionBy(F.date_trunc("month", "opened_at"), "agent_id").orderBy("prior_csat_12h", "id")
w_prior_rows_12h_prior_csat_12 = Window.partitionBy("prior_rows_12h", "prior_csat_12h_quintile")
w_prior_csat_12h_quintile_agen = Window.partitionBy("prior_csat_12h_quintile", "agent_id").orderBy(F.col("prior_csat_12h").asc_nulls_last(), "id").rowsBetween(-6, Window.currentRow)
w_prior_csat_12h_quintile_agen_2 = Window.partitionBy("prior_csat_12h_quintile", "agent_id").orderBy(F.col("prior_csat_12h").asc_nulls_last(), "id").rowsBetween(-3, -1)

in_window = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.csat").alias("prior_csat_12h"),
    )
)

with_trends = (
    in_window
    .select(
        "*",
        F.min("prior_csat_12h").over(w_agent_id).alias("agent_id_min_prior_csat_12h"),
        F.first("prior_rows_12h").over(w_opened_at_month_agent_id_by).alias("first_prior_rows_12h"),
        F.sum("prior_csat_12h").over(w_agent_id).alias("agent_id_sum_prior_csat_12h"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.dense_rank().over(w_opened_at_month_agent_id_by_2).alias("prior_csat_12h_dense_rank"),
        F.ntile(5).over(w_opened_at_month_agent_id_by_2).alias("prior_csat_12h_quintile"),
    )
)

with_trends_2 = (
    with_trends
    .select(
        "*",
        F.round(F.col("agent_id_min_prior_csat_12h") / F.nullif(F.sum("agent_id_min_prior_csat_12h").over(w_prior_rows_12h_prior_csat_12), F.lit(0)), 4).alias("agent_id_min_prior_csat_12h_share_of_prior_rows_"),
        F.sum("first_prior_rows_12h").over(w_prior_csat_12h_quintile_agen).alias("first_prior_rows_12h_sum_7"),
        F.sum("prior_csat_12h").over(w_prior_csat_12h_quintile_agen_2).alias("prior_csat_12h_sum_3_prior"),
        F.round(F.col("prior_csat_12h") / F.nullif(F.sum("prior_csat_12h").over(w_prior_rows_12h_prior_csat_12), F.lit(0)), 4).alias("prior_csat_12h_share_of_prior_rows_12h_prior_csa"),
        F.round(F.avg("prior_csat_12h_dense_rank").over(w_prior_rows_12h_prior_csat_12), 2).alias("prior_rows_12h_prior_csat_12h_quintile_avg_prior"),
    )
)

result = (
    with_trends_2
    .filter(F.col("first_prior_rows_12h") <= 7)
    .select(
        "id",
        "agent_id",
        "prior_rows_12h",
        "prior_csat_12h",
        "agent_id_min_prior_csat_12h",
        "first_prior_rows_12h",
        "agent_id_sum_prior_csat_12h",
        "rows_in_agent_id",
        "prior_csat_12h_dense_rank",
        "agent_id_min_prior_csat_12h_share_of_prior_rows_",
        "prior_csat_12h_sum_3_prior",
    )
    .orderBy(
        F.col("agent_id_sum_prior_csat_12h").asc_nulls_last(),
        F.col("prior_csat_12h_dense_rank").desc(),
    )
)
