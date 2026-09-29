from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("channel").orderBy(F.col("requester_id").desc(), F.col("id").desc())

valid_tickets = (
    df_tickets
    .filter(F.col("channel") == "chat")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "tags",
        F.concat_ws("|", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
        F.quarter(F.col("closed_at")).alias("closed_at_quarter"),
    )
)

scored = (
    valid_tickets
    .select(
        "*",
        (F.col("csat_filled") - F.col("requester_id")).alias("csat_filled_minus_requester_id"),
    )
)

latest_per_key = (
    scored
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

rollup_base = (
    latest_per_key
    .groupBy("closed_at_quarter", "csat_filled", "channel")
    .agg(
        F.percentile_approx("csat_filled", 0.5).alias("median_csat_filled"),
        F.sum("requester_id").alias("total_requester_id"),
        F.count("*").alias("n_rows"),
    )
)

tagged = (
    rollup_base
    .select(
        "*",
        F.coalesce(F.col("n_rows"), F.lit(0)).alias("n_rows_filled"),
        F.abs(F.col("closed_at_quarter") - 3).alias("closed_at_quarter_distance"),
        F.coalesce(F.col("n_rows"), F.lit(0)).alias("n_rows_filled_2"),
    )
)

result = (
    tagged
    .select(
        "closed_at_quarter",
        "csat_filled",
        "median_csat_filled",
        "n_rows_filled_2",
        "closed_at_quarter_distance",
        "total_requester_id",
    )
    .unpivot(
        ids=["closed_at_quarter", "csat_filled"],
        values=["median_csat_filled", "n_rows_filled_2", "closed_at_quarter_distance", "total_requester_id"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("closed_at_quarter", "csat_filled", "metric_name")
)
