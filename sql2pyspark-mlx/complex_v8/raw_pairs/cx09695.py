from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_id").orderBy(F.col("opened_at_month").desc(), F.col("id").desc())
w_all_by_closed_at_plus_2h = Window.orderBy(F.col("closed_at_plus_2h").asc_nulls_last(), "id")
w_csat_priority = Window.partitionBy("csat", "priority")
w_priority_closed_at_plus_2h_s = Window.partitionBy("priority", "closed_at_plus_2h_seq").orderBy(F.col("requester_id_vs_csat_priority_avg").desc(), "id")
w_priority_closed_at_plus_2h_s_2 = Window.partitionBy("priority", "closed_at_plus_2h_seq").orderBy(F.col("requester_id_vs_csat_priority_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_priority_closed_at_plus_2h_s_3 = Window.partitionBy("priority", "closed_at_plus_2h_seq").orderBy(F.col("requester_id_vs_csat_priority_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid = (
    df_tickets
    .filter(F.col("channel") == "phone")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 3600, 1).alias("hours_closed_at_to_opened_at"),
        F.date_trunc("month", F.col("opened_at")).alias("opened_at_month"),
        (F.col("closed_at") + F.expr("INTERVAL 2 HOURS")).alias("closed_at_plus_2h"),
    )
)

deduped = (
    valid
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows = (
    deduped
    .select(
        "id",
        "requester_id",
        "opened_at",
        "priority",
        "hours_closed_at_to_opened_at",
        F.row_number().over(w_all_by_closed_at_plus_2h).alias("closed_at_plus_2h_seq"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_csat_priority), 2).alias("requester_id_vs_csat_priority_avg"),
        F.size(F.collect_set("channel").over(w_csat_priority)).alias("distinct_channel_in_csat_priority"),
    )
)

with_windows_2 = (
    with_windows
    .select(
        "id",
        "requester_id",
        "opened_at",
        "priority",
        "hours_closed_at_to_opened_at",
        "requester_id_vs_csat_priority_avg",
        F.row_number().over(w_priority_closed_at_plus_2h_s).alias("requester_id_vs_csat_priority_avg_seq"),
        F.max("requester_id").over(w_priority_closed_at_plus_2h_s_2).alias("max_requester_id_so_far"),
        (F.col("requester_id") - F.lag("requester_id").over(w_priority_closed_at_plus_2h_s)).alias("requester_id_change"),
        F.last("closed_at_plus_2h_seq").over(w_priority_closed_at_plus_2h_s_3).alias("last_closed_at_plus_2h_seq"),
    )
)

result = (
    with_windows_2
    .groupingSets(
        [["requester_id_vs_csat_priority_avg_seq", "priority"], ["requester_id_vs_csat_priority_avg_seq"], ["priority"], []],
        "requester_id_vs_csat_priority_avg_seq", "priority",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min("opened_at").alias("first_opened_at"),
        F.round(100 * F.count(F.when(F.col("priority") != "P3", 1)) / F.count("*"), 1).alias("pct_priority_p3"),
    )
    .select(
        "requester_id_vs_csat_priority_avg_seq",
        F.coalesce(F.col("priority"), F.lit("ALL")).alias("priority"),
        "cnt",
        "first_opened_at",
        "pct_priority_p3",
    )
)
