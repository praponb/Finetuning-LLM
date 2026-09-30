from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_channel = Window.partitionBy("csat", "channel")
w_priority_agent_id_by_opened = Window.partitionBy("priority", "agent_id").orderBy(F.col("opened_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tickets = (
    df_tickets
    .filter(F.col("csat") >= 3)
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.when(F.col("priority").isin("P1", "P2"), "group_a").when(F.col("priority") == "P3", "group_b").otherwise("other").alias("priority_group"),
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
)

ranked = (
    scoped_tickets
    .select(
        "*",
        F.round(F.col("csat") - F.avg("csat").over(w_csat_channel), 2).alias("csat_vs_csat_channel_avg"),
        F.round(F.col("requester_id_bucket") - F.avg("requester_id_bucket").over(w_csat_channel), 2).alias("requester_id_bucket_vs_csat_channel_avg"),
        F.first("closed_at", ignorenulls=True).over(w_priority_agent_id_by_opened).alias("first_known_closed_at"),
    )
)

totals = (
    ranked
    .groupBy(
        F.year("opened_at").alias("opened_at_year"),
        F.date_trunc("month", "closed_at").alias("closed_at_month"),
    )
    .agg(
        F.round(F.sum("requester_id_bucket") / F.nullif(F.sum("requester_id_bucket_vs_csat_channel_avg"), F.lit(0)), 3).alias("requester_id_bucket_to_requester_id_bucket_vs_cs"),
        F.round(F.sum("requester_id_bucket_vs_csat_channel_avg") / F.nullif(F.sum("csat_vs_csat_channel_avg"), F.lit(0)), 3).alias("requester_id_bucket_vs_csat_channel_avg_to_csat_"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    totals
    .select(
        "opened_at_year",
        "closed_at_month",
        F.col("requester_id_bucket_vs_csat_channel_avg_to_csat_").alias("requester_id_bucket_vs_csat_channel_avg_to_csat__2"),
        F.col("requester_id_bucket_to_requester_id_bucket_vs_cs").alias("m_requester_id_bucket_to_requester_id_bucket_vs_"),
    )
    .unpivot(
        ids=["opened_at_year", "closed_at_month"],
        values=["requester_id_bucket_vs_csat_channel_avg_to_csat__2", "m_requester_id_bucket_to_requester_id_bucket_vs_"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
