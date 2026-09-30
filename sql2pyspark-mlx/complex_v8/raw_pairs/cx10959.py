from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_by_opened_at = Window.partitionBy("priority").orderBy("opened_at", "id")
w_closed_at_plus_30m_month = Window.partitionBy(F.month("closed_at_plus_30m"))

scoped = (
    df_tickets
    .filter((F.col("csat").between(2, 5)) & (F.col("priority") != "P4"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.concat(F.col("priority"), F.lit("#"), F.col("requester_id").cast("string")).alias("priority_ref"),
        F.when(F.col("csat") >= 4, "A").when(F.col("csat") >= 2, "B").otherwise("C").alias("csat_tier"),
        (F.col("closed_at") + F.expr("INTERVAL 30 MINUTES")).alias("closed_at_plus_30m"),
    )
)

metrics = (
    scoped
    .select(
        "*",
        F.ntile(5).over(w_priority_by_opened_at).alias("opened_at_quintile"),
        F.lead("closed_at").over(w_priority_by_opened_at).alias("next_closed_at"),
        F.count(F.lit(1)).over(w_closed_at_plus_30m_month).alias("rows_in_closed_at_plus_30m_month"),
    )
)

result = (
    metrics
    .select(
        "id",
        "opened_at_quintile",
        "csat",
        "rows_in_closed_at_plus_30m_month",
    )
    .unpivot(
        ids=["id"],
        values=["opened_at_quintile", "csat", "rows_in_closed_at_plus_30m_month"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
