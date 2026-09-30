from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_3d_by_prior_rows = Window.partitionBy("prior_rows_3d").orderBy(F.col("prior_rows_3d").desc_nulls_first(), "id")
w_prior_rows_3d_by_prior_rows_2 = Window.partitionBy("prior_rows_3d").orderBy(F.col("prior_rows_3d").desc_nulls_first(), "id").rowsBetween(-3, -1)

covered = (
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

agent_id_stats = (
    covered
    .groupBy("agent_id")
    .agg(F.avg("prior_rows_3d").alias("avg_prior_rows_3d"))
)

outliers = (
    covered.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.prior_rows_3d") > F.col("g.avg_prior_rows_3d"))
    .select(
        "s.*",
        "g.avg_prior_rows_3d",
        F.round(F.col("s.prior_rows_3d") - F.col("g.avg_prior_rows_3d"), 2).alias("prior_rows_3d_minus_avg"),
    )
)

with_windows = (
    outliers
    .select(
        "id",
        "agent_id",
        "opened_at",
        "avg_prior_rows_3d",
        "prior_rows_3d_minus_avg",
        (F.col("prior_rows_3d") - F.lag("prior_rows_3d").over(w_prior_rows_3d_by_prior_rows)).alias("prior_rows_3d_change"),
        (F.col("prior_rows_3d_minus_avg") - F.lag("prior_rows_3d_minus_avg").over(w_prior_rows_3d_by_prior_rows)).alias("prior_rows_3d_minus_avg_change"),
        F.sum("avg_prior_rows_3d").over(w_prior_rows_3d_by_prior_rows_2).alias("avg_prior_rows_3d_sum_3_prior"),
    )
)

totals = (
    with_windows
    .groupBy(
        F.when(F.col("prior_rows_3d_minus_avg") >= 4.0, "A").when(F.col("prior_rows_3d_minus_avg") >= -3.2, "B").otherwise("C").alias("prior_rows_3d_minus_avg_tier"),
        F.date_trunc("month", "opened_at").alias("opened_at_month"),
    )
    .agg(
        F.round(F.sum("prior_rows_3d_minus_avg_change"), 2).alias("total_prior_rows_3d_minus_avg_change"),
        F.max("opened_at").alias("last_opened_at"),
        F.min("opened_at").alias("first_opened_at"),
        F.sum("prior_rows_3d_change").alias("sum_prior_rows_3d_change_check"),
    )
    .filter(F.col("sum_prior_rows_3d_change_check") > -5)
    .drop("sum_prior_rows_3d_change_check")
)

result = (
    totals
    .select(
        "prior_rows_3d_minus_avg_tier",
        "opened_at_month",
        "total_prior_rows_3d_minus_avg_change",
        "last_opened_at",
    )
    .orderBy(F.col("total_prior_rows_3d_minus_avg_change").desc_nulls_first())
)
