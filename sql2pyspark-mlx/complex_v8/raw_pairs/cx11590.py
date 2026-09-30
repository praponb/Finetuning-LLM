from pyspark.sql import functions as F

in_window = (
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
        F.sum("b.csat").alias("prior_csat_12h"),
    )
)

annotated = (
    in_window
    .select(
        "*",
        F.date_format(F.col("opened_at"), "yyyy-MM-dd").alias("opened_at_label"),
        F.quarter(F.col("opened_at")).alias("opened_at_quarter"),
    )
)

result = (
    annotated
    .select(
        "id",
        "prior_csat_12h",
        "opened_at_quarter",
        "prior_rows_12h",
    )
    .unpivot(
        ids=["id"],
        values=["prior_csat_12h", "opened_at_quarter", "prior_rows_12h"],
        variableColumnName="field",
        valueColumnName="reading",
    )
)
