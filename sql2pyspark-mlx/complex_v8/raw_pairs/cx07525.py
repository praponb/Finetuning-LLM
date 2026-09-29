from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at_full = Window.orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_closed_at_month = Window.partitionBy(F.month("closed_at"))
w_csat_agent_id_by_csat = Window.partitionBy("csat", "agent_id").orderBy(F.col("csat").desc(), "id")
w_closed_at_month_agent_id = Window.partitionBy(F.date_trunc("month", "closed_at"), "agent_id")
w_csat_by_csat_running = Window.partitionBy("csat").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_by_csat = Window.partitionBy("agent_id").orderBy("csat", "id")
w_agent_id_by_closed_at = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_id_by_csat_moving_2 = Window.partitionBy("agent_id").orderBy("csat", "id").rowsBetween(-2, Window.currentRow)
w_agent_id_by_closed_at_runnin = Window.partitionBy("agent_id").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_closed_at_year_agent_id = Window.partitionBy(F.year("closed_at"), "agent_id")
w_first_second_closed_at_month = Window.partitionBy(F.month("first_second_closed_at")).orderBy("stddev_rows_in_closed_at_month", "csat_quartile", "closed_at_year")
w_first_second_closed_at_month_2 = Window.partitionBy(F.month("first_second_closed_at")).orderBy("stddev_rows_in_closed_at_month", "csat_quartile", "closed_at_year").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_tickets = (
    df_tickets
    .filter((F.col("csat") < 5) & (F.col("agent_id").isNotNull()))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "csat",
        "tags",
    )
)

metrics = (
    prepared_tickets
    .select(
        "id",
        "closed_at",
        "csat",
        "tags",
        F.nth_value("closed_at", 2).over(w_all_by_closed_at_full).alias("second_closed_at"),
        F.count(F.lit(1)).over(w_closed_at_month).alias("rows_in_closed_at_month"),
        F.ntile(4).over(w_csat_agent_id_by_csat).alias("csat_quartile"),
        F.max("csat").over(w_closed_at_month_agent_id).alias("closed_at_month_agent_id_max_csat"),
        F.first("csat", ignorenulls=True).over(w_csat_by_csat_running).alias("first_known_csat"),
        F.row_number().over(w_agent_id_by_csat).alias("rn"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_closed_at_month), F.lit(0)), 4).alias("csat_share_of_closed_at_month"),
        F.dense_rank().over(w_agent_id_by_closed_at).alias("closed_at_dense_rank"),
        F.round(F.avg("csat").over(w_agent_id_by_csat_moving_2), 2).alias("csat_avg_3"),
        F.first("csat", ignorenulls=True).over(w_agent_id_by_closed_at_runnin).alias("first_known_csat_2"),
        F.min("csat").over(w_closed_at_year_agent_id).alias("closed_at_year_agent_id_min_csat"),
    )
)

totals = (
    metrics
    .groupBy("csat_quartile", F.year("closed_at").alias("closed_at_year"))
    .agg(
        F.round(F.stddev("rows_in_closed_at_month"), 2).alias("stddev_rows_in_closed_at_month"),
        F.sum("csat_share_of_closed_at_month").alias("total_csat_share_of_closed_at_month"),
        F.sum("closed_at_month_agent_id_max_csat").alias("total_closed_at_month_agent_id_max_csat"),
        F.min("second_closed_at").alias("first_second_closed_at"),
    )
)

ranked = (
    totals
    .select(
        "csat_quartile",
        "closed_at_year",
        "stddev_rows_in_closed_at_month",
        "total_csat_share_of_closed_at_month",
        "total_closed_at_month_agent_id_max_csat",
        "first_second_closed_at",
        F.ntile(3).over(w_first_second_closed_at_month).alias("stddev_rows_in_closed_at_month_tercile"),
        F.count(F.lit(1)).over(w_first_second_closed_at_month_2).alias("running_count"),
    )
)

result = (
    ranked
    .select(
        "csat_quartile",
        "closed_at_year",
        "stddev_rows_in_closed_at_month",
        "first_second_closed_at",
        "stddev_rows_in_closed_at_month_tercile",
        "running_count",
    )
    .orderBy(F.col("first_second_closed_at").asc(), "csat_quartile", "closed_at_year")
    .limit(10)
)
