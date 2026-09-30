from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat = Window.partitionBy("csat")
w_channel_csat_by_closed_at = Window.partitionBy("channel", "csat").orderBy(F.col("closed_at").desc(), "id")
w_all_by_csat_prior_3 = Window.orderBy("csat", "id").rowsBetween(-3, -1)
w_dedup = Window.partitionBy("priority").orderBy("closed_at", "id")

scoped_tickets = (
    df_tickets
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
        F.initcap(F.trim(F.col("priority"))).alias("priority_clean"),
        F.array_join(F.array_sort(F.col("tags")), ",").alias("tags_text"),
    )
)

metrics = (
    scoped_tickets
    .select(
        "*",
        F.size(F.collect_set("agent_id").over(w_csat)).alias("distinct_agent_id_in_csat"),
        F.lead("csat", 1, 0).over(w_channel_csat_by_closed_at).alias("next_csat"),
        F.min("requester_id").over(w_csat).alias("csat_min_requester_id"),
        F.max("csat").over(w_all_by_csat_prior_3).alias("csat_max_3_prior"),
    )
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

qualified = (
    deduped
    .crossJoin(deduped.agg(F.max("csat").alias("overall_max_csat")))
    .filter(F.col("csat") < F.col("overall_max_csat"))
    .drop("overall_max_csat")
)

result = (
    qualified
    .select(
        "id",
        F.col("csat_max_3_prior").alias("csat_max_3_prior_v"),
        F.col("csat").alias("csat_v"),
        F.col("distinct_agent_id_in_csat").alias("distinct_agent_id_in_csat_v"),
    )
    .unpivot(
        ids=["id"],
        values=["csat_max_3_prior_v", "csat_v", "distinct_agent_id_in_csat_v"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
)
