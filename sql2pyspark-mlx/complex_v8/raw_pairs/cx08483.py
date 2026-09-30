from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id", "priority_or_unknown").orderBy(F.col("opened_at").desc(), F.col("id").desc())

base_tickets = (
    df_tickets
    .filter((~F.array_contains(F.col("tags"), "outage")) & (F.col("priority").isNotNull()))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.date_format(F.col("closed_at"), "yyyy-MM").alias("closed_at_label"),
        F.date_trunc("month", F.col("opened_at")).alias("opened_at_month"),
    )
)

derived = (
    base_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "csat",
        "closed_at_label",
        "opened_at_month",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.upper(F.trim(F.col("closed_at_label"))).alias("closed_at_label_clean"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown_2"),
    )
)

latest_per_key = (
    derived
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

stats = (
    latest_per_key
    .groupBy("agent_id", "priority_or_unknown_2")
    .agg(
        F.percentile_approx("csat", 0.5).alias("median_csat"),
        F.max("opened_at").alias("last_opened_at"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    stats
    .select(
        "agent_id",
        "priority_or_unknown_2",
        "n_rows",
        "median_csat",
    )
    .unpivot(
        ids=["agent_id", "priority_or_unknown_2"],
        values=["n_rows", "median_csat"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
