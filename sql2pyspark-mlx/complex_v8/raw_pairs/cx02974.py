from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat = Window.partitionBy("csat")
w_all_by_csat = Window.orderBy(F.col("csat").desc_nulls_first(), "id")

filtered_tickets = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "login"))
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
        F.least(F.col("csat"), F.col("requester_id")).alias("least_csat_requester_id"),
    )
)

ranked = (
    filtered_tickets
    .select(
        "id",
        "requester_id",
        "opened_at",
        "closed_at",
        "priority",
        "tags",
        "least_csat_requester_id",
        F.round(F.col("least_csat_requester_id") / F.nullif(F.sum("least_csat_requester_id").over(w_csat), F.lit(0)), 4).alias("least_csat_requester_id_share_of_csat"),
        F.round(F.cume_dist().over(w_all_by_csat), 3).alias("csat_cume_dist"),
        F.min("requester_id").over(w_csat).alias("csat_min_requester_id"),
        F.size(F.collect_set("priority").over(w_csat)).alias("distinct_priority_in_csat"),
    )
)

result = (
    ranked
    .groupingSets(
        [["priority", "distinct_priority_in_csat"], ["priority"]],
        "priority", "distinct_priority_in_csat",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("priority").alias("distinct_prioritys"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("priority").asc_nulls_last(),
        F.col("distinct_priority_in_csat").asc_nulls_last(),
    )
)
