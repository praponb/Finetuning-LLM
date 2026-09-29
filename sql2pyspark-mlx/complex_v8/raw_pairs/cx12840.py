from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_at_quarter_by_opened = Window.partitionBy("opened_at_quarter").orderBy(F.col("opened_at_quarter").desc(), "id").rowsBetween(-1, Window.currentRow)
w_opened_at_quarter_by_opened_2 = Window.partitionBy("opened_at_quarter").orderBy(F.col("opened_at_quarter").desc(), "id")
w_prior_rows_12h_agent_id = Window.partitionBy("prior_rows_12h", "agent_id")
w_all_by_opened_at_full = Window.orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

joined_ranges = (
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
        F.sum("b.requester_id").alias("prior_requester_id_12h"),
    )
)

annotated = (
    joined_ranges
    .filter(F.col("opened_at") < F.lit("2025-02-24 08:00:00").cast("timestamp"))
    .select(
        "*",
        F.quarter(F.col("opened_at")).alias("opened_at_quarter"),
        F.dayofweek(F.col("opened_at")).alias("opened_at_dayofweek"),
        (F.floor(F.col("prior_requester_id_12h") / 25) * 25).alias("prior_requester_id_12h_bucket"),
    )
)

metrics = (
    annotated
    .select(
        "*",
        F.round(F.avg("opened_at_dayofweek").over(w_opened_at_quarter_by_opened), 2).alias("opened_at_dayofweek_avg_2"),
        (F.col("prior_rows_12h") - F.lag("prior_rows_12h").over(w_opened_at_quarter_by_opened_2)).alias("prior_rows_12h_change"),
        F.first("opened_at").over(w_all_by_opened_at_full).alias("first_opened_at"),
        F.lead("opened_at_dayofweek").over(w_opened_at_quarter_by_opened_2).alias("next_opened_at_dayofweek"),
        F.lead("prior_requester_id_12h").over(w_opened_at_quarter_by_opened_2).alias("next_prior_requester_id_12h"),
    )
)

qualified = (
    metrics
    .crossJoin(metrics.agg(F.max("next_prior_requester_id_12h").alias("overall_max_next_prior_requester_id_12h")))
    .filter(F.col("next_prior_requester_id_12h") < F.col("overall_max_next_prior_requester_id_12h"))
    .drop("overall_max_next_prior_requester_id_12h")
)

result = (
    qualified
    .select(
        "id",
        "agent_id",
        "opened_at",
        "prior_requester_id_12h",
        "opened_at_dayofweek",
        "opened_at_dayofweek_avg_2",
        "prior_rows_12h_change",
        "first_opened_at",
        "next_opened_at_dayofweek",
    )
)
