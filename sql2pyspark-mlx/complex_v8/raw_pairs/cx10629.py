from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_csat = Window.partitionBy("priority", "csat")
w_closed_at_to_date_csat = Window.partitionBy(F.to_date("closed_at"), "csat")
w_priority_by_csat_ties = Window.partitionBy("priority").orderBy(F.col("csat").asc_nulls_last())
w_closed_at_year_csat_by_opene = Window.partitionBy(F.year("closed_at"), "csat").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_by_csat = Window.partitionBy("priority").orderBy(F.col("csat").asc_nulls_last(), "id")
w_csat_opened_at_month = Window.partitionBy("csat", F.month("opened_at"))
w_agent_id_csat_by_csat_prior = Window.partitionBy("agent_id", "csat").orderBy("csat", "id").rowsBetween(-6, -1)
w_agent_id_csat_by_opened_at_f = Window.partitionBy("agent_id", "csat").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_csat_by_csat = Window.partitionBy("agent_id", "csat").orderBy("csat", "id")
w_priority_by_csat_running = Window.partitionBy("priority").orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tickets
    .filter((F.col("agent_id") <= 2) | (F.col("closed_at") < F.lit("2025-02-19 21:00:00").cast("timestamp")))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
    )
)

windowed = (
    prepared
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "tags",
        F.round(F.col("csat") - F.avg("csat").over(w_priority_csat), 2).alias("csat_vs_priority_csat_avg"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_closed_at_to_date_csat), F.lit(0)), 4).alias("csat_share_of_closed_at_to_date_csat"),
        F.rank().over(w_priority_by_csat_ties).alias("csat_rank"),
        F.sum("csat").over(w_closed_at_year_csat_by_opene).alias("running_csat"),
        F.row_number().over(w_priority_by_csat).alias("csat_seq"),
        F.count(F.lit(1)).over(w_csat_opened_at_month).alias("rows_in_csat_opened_at_month"),
        F.round(F.avg("csat").over(w_agent_id_csat_by_csat_prior), 2).alias("csat_avg_6_prior"),
        F.nth_value("opened_at", 2).over(w_agent_id_csat_by_opened_at_f).alias("second_opened_at"),
        F.lag("closed_at", 3).over(w_agent_id_csat_by_csat).alias("prev3_closed_at"),
        F.min("csat").over(w_priority_by_csat_running).alias("min_csat_so_far"),
    )
)

result = (
    windowed
    .select(
        "id",
        "opened_at",
        "csat",
        "tags",
        "csat_vs_priority_csat_avg",
        "csat_rank",
        "running_csat",
        "csat_seq",
        "rows_in_csat_opened_at_month",
        "prev3_closed_at",
        "min_csat_so_far",
    )
    .orderBy(F.col("min_csat_so_far").asc_nulls_last(), F.col("csat_rank").asc())
)
