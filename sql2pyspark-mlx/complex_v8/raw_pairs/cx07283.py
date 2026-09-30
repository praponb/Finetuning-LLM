from pyspark.sql import functions as F

covered = (
    df_tickets.alias("a")
    .join(
        df_tickets.alias("b"),
        (F.col("b.agent_id") == F.col("a.agent_id"))
        & (F.col("b.opened_at") < F.col("a.opened_at"))
        & (F.col("b.opened_at") >= F.col("a.opened_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.agent_id", "a.opened_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.requester_id").alias("prior_requester_id_1d"),
    )
)

without_matches = (
    covered.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.csat") > 1) & (F.col("t.priority") == "P1"), "left_anti")
)

result = (
    without_matches
    .groupingSets(
        [["prior_rows_1d", "agent_id"], ["prior_rows_1d"], ["agent_id"], []],
        "prior_rows_1d", "agent_id",
    )
    .agg(
        F.max("opened_at").alias("last_opened_at"),
        F.min("opened_at").alias("first_opened_at"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("prior_rows_1d").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
    )
)
