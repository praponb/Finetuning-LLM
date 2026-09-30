from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_by_prior_requester = Window.partitionBy("agent_id").orderBy("prior_requester_id_12h")
w_prior_rows_12h_by_opened_at = Window.partitionBy("prior_rows_12h").orderBy(F.col("opened_at").desc(), "id")
w_prior_rows_12h_by_opened_at_2 = Window.partitionBy("prior_rows_12h").orderBy(F.col("opened_at").desc(), "id").rowsBetween(-6, Window.currentRow)
w_agent_id = Window.partitionBy("agent_id")
w_prior_rows_12h_by_opened_at_3 = Window.partitionBy("prior_rows_12h").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

matched = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.requester_id").alias("prior_requester_id_12h"),
    )
)

sequenced = (
    matched
    .select(
        "*",
        F.rank().over(w_agent_id_by_prior_requester).alias("prior_requester_id_12h_rank"),
        (F.col("prior_rows_12h") - F.lag("prior_rows_12h").over(w_prior_rows_12h_by_opened_at)).alias("prior_rows_12h_change"),
        F.max("prior_rows_12h").over(w_prior_rows_12h_by_opened_at_2).alias("prior_rows_12h_max_7"),
        F.round(F.col("prior_rows_12h") / F.nullif(F.sum("prior_rows_12h").over(w_agent_id), F.lit(0)), 4).alias("prior_rows_12h_share_of_agent_id"),
        F.count(F.lit(1)).over(w_prior_rows_12h_by_opened_at_3).alias("running_count"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "agent_id",
        "opened_at",
        "prior_rows_12h",
        "prior_requester_id_12h_rank",
        "prior_rows_12h_change",
        "prior_rows_12h_max_7",
        "prior_rows_12h_share_of_agent_id",
    )
)
