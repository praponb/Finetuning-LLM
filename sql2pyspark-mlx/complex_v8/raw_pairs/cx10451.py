from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at_ties = Window.orderBy("closed_at")
w_all_by_closed_at = Window.orderBy("closed_at", "id")
w_csat = Window.partitionBy("csat")
w_agent_id_by_closed_at_full = Window.partitionBy("agent_id").orderBy(F.col("closed_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_closed_at_rank_by_c = Window.partitionBy("agent_id", "closed_at_rank").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_closed_at_dense_rank_priorit = Window.partitionBy("closed_at_dense_rank", "priority").orderBy(F.col("closed_at_dense_rank").asc_nulls_last(), "id")

scoped_tickets = (
    df_tickets
    .filter((~F.col("priority").isin("P2", "P3")) & (F.col("agent_id").isNotNull()))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
    )
)

metrics = (
    scoped_tickets
    .withColumn("closed_at_rank", F.rank().over(w_all_by_closed_at_ties))
    .withColumn("closed_at_dense_rank", F.dense_rank().over(w_all_by_closed_at))
)

with_windows = (
    metrics
    .select(
        "*",
        F.min("closed_at_rank").over(w_csat).alias("csat_min_closed_at_rank"),
        F.count(F.lit(1)).over(w_csat).alias("rows_in_csat"),
        F.last("closed_at_rank").over(w_agent_id_by_closed_at_full).alias("last_closed_at_rank"),
        F.max("closed_at_dense_rank").over(w_csat).alias("csat_max_closed_at_dense_rank"),
        F.nth_value("closed_at", 2).over(w_agent_id_closed_at_rank_by_c).alias("second_closed_at"),
        F.lead("closed_at_rank", 1, 0).over(w_closed_at_dense_rank_priorit).alias("next_closed_at_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "closed_at_dense_rank",
        "next_closed_at_rank",
        "csat_min_closed_at_rank",
    )
    .unpivot(
        ids=["id"],
        values=["closed_at_dense_rank", "next_closed_at_rank", "csat_min_closed_at_rank"],
        variableColumnName="field",
        valueColumnName="amount",
    )
)
