from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_by_closed_at = Window.partitionBy("csat").orderBy(F.col("closed_at").desc(), "id")
w_csat_by_closed_at_running = Window.partitionBy("csat").orderBy(F.col("closed_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id = Window.partitionBy("agent_id")

valid = (
    df_tickets
    .filter(F.size(F.col("tags")) >= 1)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
    )
)

with_trends = (
    valid
    .select(
        "*",
        F.rank().over(w_csat_by_closed_at).alias("closed_at_rank"),
        F.sum("requester_id").over(w_csat_by_closed_at_running).alias("running_requester_id"),
        F.sum("csat").over(w_agent_id).alias("agent_id_sum_csat"),
    )
)

scored = (
    with_trends
    .select(
        "*",
        F.concat(F.col("priority"), F.lit("#"), F.col("agent_id").cast("string")).alias("priority_ref"),
        (F.col("opened_at") + F.expr("INTERVAL 2 HOURS")).alias("opened_at_plus_2h"),
    )
)

result = (
    scored
    .rollup("priority", "agent_id", "closed_at_rank")
    .agg(
        F.sum(F.when(~F.col("priority").isin("P1", "P2"), F.col("csat")).otherwise(0)).alias("csat_priority_not_in_p1_p2"),
    )
    .orderBy(
        F.col("priority").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
        F.col("closed_at_rank").asc_nulls_last(),
    )
)
