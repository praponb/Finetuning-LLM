from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id")
w_csat_by_hours_opened_at_to_c = Window.partitionBy("csat").orderBy(F.col("hours_opened_at_to_closed_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_csat = Window.partitionBy("agent_id").orderBy("csat", "id")
w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_csat_by_hours_opened_at_to_c_2 = Window.partitionBy("csat").orderBy(F.col("hours_opened_at_to_closed_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_tickets
    .filter((F.col("channel").isin("web", "phone")) & (F.col("agent_id") <= 6))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("closed_at")) - F.unix_timestamp(F.col("opened_at"))) / 3600, 1).alias("hours_opened_at_to_closed_at"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.min("hours_opened_at_to_closed_at").over(w_agent_id).alias("agent_id_min_hours_opened_at_to_closed_at"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id), 2).alias("csat_vs_agent_id_avg"),
        F.nth_value("requester_id", 2).over(w_csat_by_hours_opened_at_to_c).alias("second_requester_id"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.lead("csat", 3, 0).over(w_agent_id_by_csat).alias("next3_csat"),
        F.max("requester_id").over(w_agent_id_csat).alias("agent_id_csat_max_requester_id"),
        F.sum("requester_id").over(w_csat_by_hours_opened_at_to_c_2).alias("running_requester_id"),
        (F.col("hours_opened_at_to_closed_at") - F.lag("hours_opened_at_to_closed_at").over(w_agent_id_by_csat)).alias("hours_opened_at_to_closed_at_change"),
    )
)

derived = (
    with_windows
    .filter(F.col("hours_opened_at_to_closed_at_change") >= -1400.0)
    .select(
        "*",
        F.coalesce(F.col("next3_csat"), F.lit(0)).alias("next3_csat_filled"),
        (F.col("csat") + F.col("requester_id")).alias("csat_plus_requester_id"),
    )
)

result = (
    derived
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "tags",
        "hours_opened_at_to_closed_at",
        "agent_id_min_hours_opened_at_to_closed_at",
        "csat_vs_agent_id_avg",
        "second_requester_id",
        "next3_csat",
        "agent_id_csat_max_requester_id",
        "hours_opened_at_to_closed_at_change",
        "csat_plus_requester_id",
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
    )
    .orderBy(F.col("agent_id").desc())
)
