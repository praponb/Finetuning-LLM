from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_by_opened_at_full = Window.partitionBy("priority").orderBy(F.col("opened_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_by_opened_at_runnin = Window.partitionBy("priority").orderBy(F.col("opened_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_opened_at_month_agent_id = Window.partitionBy(F.date_trunc("month", "opened_at"), "agent_id")
w_agent_id_by_requester_id_min = Window.partitionBy("agent_id").orderBy("requester_id_minus_csat")
w_all_by_max_csat_plus_request = Window.orderBy(F.col("max_csat_plus_requester_id").desc(), "opened_at_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_max_csat_plus_request_2 = Window.orderBy(F.col("max_csat_plus_requester_id").desc(), "opened_at_month")

scoped = (
    df_tickets
    .filter(~F.array_contains(F.col("tags"), "login"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "tags",
        (F.col("csat") + F.col("requester_id")).alias("csat_plus_requester_id"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
        (F.col("requester_id") - F.col("csat")).alias("requester_id_minus_csat"),
    )
)

ranked = (
    scoped
    .select(
        "*",
        F.nth_value("csat_per_requester_id", 2).over(w_priority_by_opened_at_full).alias("second_csat_per_requester_id"),
        F.first("csat_plus_requester_id", ignorenulls=True).over(w_priority_by_opened_at_runnin).alias("first_known_csat_plus_requester_id"),
        F.round(F.col("csat_plus_requester_id") / F.nullif(F.sum("csat_plus_requester_id").over(w_opened_at_month_agent_id), F.lit(0)), 4).alias("csat_plus_requester_id_share_of_opened_at_month_"),
        F.rank().over(w_agent_id_by_requester_id_min).alias("requester_id_minus_csat_rank"),
    )
)

per_group = (
    ranked
    .groupBy(F.date_trunc("month", "opened_at").alias("opened_at_month"))
    .agg(
        F.count(F.when(F.col("priority") == "P4", 1)).alias("n_priority_p4"),
        F.max("csat_plus_requester_id").alias("max_csat_plus_requester_id"),
        F.sum(F.when(F.col("priority").isin("P1", "P4"), 1).otherwise(0)).alias("n_priority_in_p1_p4"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 3)
)

metrics = (
    per_group
    .withColumn("second_n_priority_p4", F.nth_value("n_priority_p4", 2).over(w_all_by_max_csat_plus_request))
    .withColumn("max_csat_plus_requester_id_tercile", F.ntile(3).over(w_all_by_max_csat_plus_request_2))
)

result = (
    metrics
    .select(
        "opened_at_month",
        "n_priority_p4",
        "n_priority_in_p1_p4",
        "n_rows",
        "second_n_priority_p4",
        "max_csat_plus_requester_id_tercile",
        (F.col("n_rows") + F.col("n_priority_p4")).alias("n_rows_plus_n_priority_p4"),
        F.least(F.col("n_priority_p4"), F.col("n_rows")).alias("least_n_priority_p4_n_rows"),
    )
    .orderBy(
        F.col("n_rows").asc_nulls_last(),
        F.col("n_rows_plus_n_priority_p4").desc(),
    )
)
