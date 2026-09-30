from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_year = Window.partitionBy(F.year("closed_at"))
w_agent_id_csat_by_csat_prior = Window.partitionBy("agent_id", "csat").orderBy(F.col("csat").desc(), "id").rowsBetween(-1, -1)
w_agent_id_by_opened_at = Window.partitionBy("agent_id").orderBy(F.col("opened_at").asc_nulls_last(), "id")
w_agent_id_by_opened_at_runnin = Window.partitionBy("agent_id").orderBy(F.col("opened_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_closed_at = Window.orderBy("closed_at", "id")
w_opened_at_year_agent_id_by_o = Window.partitionBy(F.year("opened_at"), "agent_id").orderBy("opened_at")
w_csat_agent_id = Window.partitionBy("csat", "agent_id")
w_rn_csat_by_rn_full = Window.partitionBy("rn", "csat").orderBy(F.col("rn").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_agent_id_in_closed = Window.partitionBy("distinct_agent_id_in_closed_at_year").orderBy("last_known_closed_at", "id").rowsBetween(-3, -1)
w_first_known_csat_closed_at_m = Window.partitionBy("first_known_csat", F.date_trunc("month", "closed_at"))

valid_tickets = (
    df_tickets
    .filter(F.col("csat") < 4)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.when(F.col("csat") >= 4, "A").when(F.col("csat") >= 2, "B").otherwise("C").alias("csat_tier"),
    )
)

joined = (
    valid_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.csat",
        "su.agent_name",
    )
)

with_windows = (
    joined
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "agent_name",
        F.round(F.col("csat") - F.avg("csat").over(w_closed_at_year), 2).alias("csat_vs_closed_at_year_avg"),
        F.size(F.collect_set("agent_id").over(w_closed_at_year)).alias("distinct_agent_id_in_closed_at_year"),
        F.max("csat").over(w_agent_id_csat_by_csat_prior).alias("csat_max_1_prior"),
        F.row_number().over(w_agent_id_by_opened_at).alias("rn"),
        F.last("closed_at", ignorenulls=True).over(w_agent_id_by_opened_at_runnin).alias("last_known_closed_at"),
        (F.col("csat") - F.lag("csat").over(w_all_by_closed_at)).alias("csat_change"),
        F.round(F.cume_dist().over(w_opened_at_year_agent_id_by_o), 3).alias("opened_at_cume_dist"),
        F.count(F.lit(1)).over(w_csat_agent_id).alias("rows_in_csat_agent_id"),
        F.first("csat", ignorenulls=True).over(w_agent_id_by_opened_at_runnin).alias("first_known_csat"),
        F.min("csat").over(w_closed_at_year).alias("closed_at_year_min_csat"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "agent_id",
        "closed_at",
        "csat",
        "agent_name",
        "csat_vs_closed_at_year_avg",
        "csat_max_1_prior",
        "rn",
        "last_known_closed_at",
        "opened_at_cume_dist",
        "rows_in_csat_agent_id",
        "first_known_csat",
        F.nth_value("last_known_closed_at", 2).over(w_rn_csat_by_rn_full).alias("second_last_known_closed_at"),
        F.max("rn").over(w_distinct_agent_id_in_closed).alias("rn_max_3_prior"),
        F.max("opened_at_cume_dist").over(w_first_known_csat_closed_at_m).alias("first_known_csat_closed_at_month_max_opened_at_c"),
        F.size(F.collect_set("agent_id").over(w_first_known_csat_closed_at_m)).alias("distinct_agent_id_in_first_known_csat_closed_at_"),
    )
)

result = (
    metrics
    .select(
        "id",
        "agent_id",
        "closed_at",
        "agent_name",
        "csat_vs_closed_at_year_avg",
        "csat_max_1_prior",
        "rn",
        "opened_at_cume_dist",
        "rows_in_csat_agent_id",
        "first_known_csat",
        "rn_max_3_prior",
        "first_known_csat_closed_at_month_max_opened_at_c",
        "distinct_agent_id_in_first_known_csat_closed_at_",
        (F.col("rows_in_csat_agent_id") - F.col("rn_max_3_prior")).alias("rows_in_csat_agent_id_minus_rn_max_3_prior"),
        (F.col("closed_at") + F.expr("INTERVAL 15 MINUTES")).alias("closed_at_plus_15m"),
    )
    .orderBy("rn", F.col("id").desc())
)
