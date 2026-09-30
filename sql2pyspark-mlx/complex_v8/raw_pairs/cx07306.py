from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_agent_hired_on_runnin = Window.orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_agent_hired_on_year = Window.partitionBy("agent_id", F.year("agent_hired_on"))
w_agent_id_priority_group_by_c = Window.partitionBy("agent_id", "priority_group").orderBy(F.col("closed_at").desc_nulls_first(), "id")
w_all_by_agent_hired_on = Window.orderBy("agent_hired_on", "id")
w_agent_id_by_agent_hired_on_f = Window.partitionBy("agent_id").orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_agent_hired_on_t = Window.partitionBy("agent_id").orderBy(F.col("agent_hired_on").desc())
w_agent_id_priority_group_by_c_2 = Window.partitionBy("agent_id", "priority_group").orderBy(F.col("closed_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_closed_at_to_date_priority_g = Window.partitionBy(F.to_date("closed_at"), "priority_group")
w_priority_group_agent_hired_o = Window.partitionBy("priority_group", "agent_hired_on_rank").orderBy("first_known_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_closed_at_to_date_distinct_p = Window.partitionBy(F.to_date("closed_at"), "distinct_priority_group_in_agent_id_agent_hired_")
w_priority_group_agent_hired_o_2 = Window.partitionBy("priority_group", "agent_hired_on_rank").orderBy("first_known_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_ids_first_agent_hir = Window.partitionBy("distinct_ids", F.year("first_agent_hired_on"))
w_distinct_priority_group_in_a = Window.partitionBy("distinct_priority_group_in_agent_id_agent_hired_", "closed_at_decile").orderBy("stddev_agent_id_agent_hired_on_year_sum_requeste")
w_distinct_priority_group_in_a_2 = Window.partitionBy("distinct_priority_group_in_agent_id_agent_hired_", "closed_at_decile").orderBy("stddev_agent_id_agent_hired_on_year_sum_requeste", "distinct_priority_group_in_agent_id_agent_hired_", "agent_hired_on_seq", "closed_at_decile").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_tickets = (
    df_tickets
    .filter((F.to_date(F.col("closed_at")) < F.lit("2025-03-05").cast("date")) & (F.col("opened_at") < F.lit("2025-02-12 11:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "tags",
        F.abs(F.col("requester_id") - 12).alias("requester_id_distance"),
        F.when(F.col("priority").isin("P2", "P3"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other").alias("priority_group"),
    )
)

joined_agent = (
    valid_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.tags",
        "s.priority_group",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

ranked = (
    joined_agent
    .select(
        "*",
        F.first("requester_id", ignorenulls=True).over(w_all_by_agent_hired_on_runnin).alias("first_known_requester_id"),
        F.size(F.collect_set("priority_group").over(w_agent_id_agent_hired_on_year)).alias("distinct_priority_group_in_agent_id_agent_hired_"),
        F.ntile(10).over(w_agent_id_priority_group_by_c).alias("closed_at_decile"),
        F.sum("requester_id").over(w_agent_id_agent_hired_on_year).alias("agent_id_agent_hired_on_year_sum_requester_id"),
        F.row_number().over(w_all_by_agent_hired_on).alias("agent_hired_on_seq"),
        F.row_number().over(w_agent_id_priority_group_by_c).alias("seq_no"),
        F.nth_value("requester_id", 2).over(w_agent_id_by_agent_hired_on_f).alias("second_requester_id"),
        F.rank().over(w_agent_id_by_agent_hired_on_t).alias("agent_hired_on_rank"),
        F.min("requester_id").over(w_all_by_agent_hired_on_runnin).alias("min_requester_id_so_far"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_agent_id_priority_group_by_c))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.last("agent_hired_on").over(w_agent_id_priority_group_by_c_2).alias("last_agent_hired_on"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_closed_at_to_date_priority_g), F.lit(0)), 4).alias("requester_id_share_of_closed_at_to_date_priority"),
    )
)

windowed = (
    ranked
    .select(
        "*",
        F.max("second_requester_id").over(w_priority_group_agent_hired_o).alias("max_second_requester_id_so_far"),
        F.sum("first_known_requester_id").over(w_closed_at_to_date_distinct_p).alias("closed_at_to_date_distinct_priority_group_in_age"),
        F.nth_value("min_requester_id_so_far", 2).over(w_priority_group_agent_hired_o_2).alias("second_min_requester_id_so_far"),
    )
)

per_group = (
    windowed
    .groupBy(
        "distinct_priority_group_in_agent_id_agent_hired_",
        "agent_hired_on_seq",
        "closed_at_decile",
    )
    .agg(
        F.sum("min_requester_id_so_far").alias("total_min_requester_id_so_far"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("seq_no").alias("total_seq_no"),
        F.min("agent_hired_on").alias("first_agent_hired_on"),
        F.round(F.stddev("agent_id_agent_hired_on_year_sum_requester_id"), 2).alias("stddev_agent_id_agent_hired_on_year_sum_requeste"),
        F.max("requester_id").alias("max_requester_id"),
    )
)

metrics = (
    per_group
    .select(
        "*",
        F.max("max_requester_id").over(w_distinct_ids_first_agent_hir).alias("distinct_ids_first_agent_hired_on_year_max_max_r"),
        F.round(F.percent_rank().over(w_distinct_priority_group_in_a), 3).alias("stddev_agent_id_agent_hired_on_year_sum_requeste_2"),
        F.last("first_agent_hired_on").over(w_distinct_priority_group_in_a_2).alias("last_first_agent_hired_on"),
    )
)

result = (
    metrics
    .select(
        "distinct_priority_group_in_agent_id_agent_hired_",
        "agent_hired_on_seq",
        "closed_at_decile",
        "total_seq_no",
        "first_agent_hired_on",
        "stddev_agent_id_agent_hired_on_year_sum_requeste",
        "max_requester_id",
        "distinct_ids_first_agent_hired_on_year_max_max_r",
        "stddev_agent_id_agent_hired_on_year_sum_requeste_2",
        F.coalesce(F.col("agent_hired_on_seq"), F.lit(0)).alias("agent_hired_on_seq_filled"),
    )
)
