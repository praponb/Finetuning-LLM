from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_agent_id = Window.partitionBy("priority", "agent_id")
w_closed_at_year_month_agent_i = Window.partitionBy(F.month("closed_at_year"), "agent_id").orderBy(F.col("closed_at").desc())
w_closed_at_year_to_date_close = Window.partitionBy(F.to_date("closed_at_year"), F.date_trunc("month", "closed_at")).orderBy("csat")

scoped = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "login")) & (F.col("requester_id") >= 5))
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
        F.abs(F.col("requester_id") - 18).alias("requester_id_distance"),
    )
)

enriched = (
    scoped
    .filter(F.col("csat").between(1, 4))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
        "priority_clean",
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
        F.date_trunc("year", F.col("closed_at")).alias("closed_at_year"),
    )
)

sequenced = (
    enriched
    .select(
        "id",
        "priority",
        "csat",
        "tags",
        "priority_clean",
        "closed_at_year",
        F.rank().over(w_closed_at_year_month_agent_i).alias("closed_at_rank"),
        F.dense_rank().over(w_closed_at_year_to_date_close).alias("csat_dense_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "csat_dense_rank",
        "closed_at_rank",
        "csat",
    )
    .unpivot(
        ids=["id"],
        values=["csat_dense_rank", "closed_at_rank", "csat"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
