from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id", "channel").orderBy("closed_at", "id")

valid_tickets = (
    df_tickets
    .filter(F.month(F.col("opened_at")).isin(1, 3))
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
    )
)

unique_rows = (
    valid_tickets
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

summary = (
    unique_rows
    .groupBy("channel", F.date_trunc("month", "opened_at").alias("opened_at_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("csat") / F.nullif(F.sum("requester_id"), F.lit(0)), 3).alias("csat_to_requester_id_ratio"),
        F.max("csat").alias("max_csat"),
    )
)

filtered = (
    summary
    .crossJoin(summary.agg(F.max("max_csat").alias("overall_max_max_csat")))
    .filter(F.col("max_csat") < F.col("overall_max_max_csat"))
    .drop("overall_max_max_csat")
)

result = (
    filtered
    .select(
        "channel",
        "opened_at_month",
        "row_count",
        "distinct_ids",
        "max_csat",
    )
    .unpivot(
        ids=["channel", "opened_at_month"],
        values=["row_count", "distinct_ids", "max_csat"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .orderBy("channel", "opened_at_month", "measure")
)
