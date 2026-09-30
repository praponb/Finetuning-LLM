from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")
w_channel = Window.partitionBy("channel")

recent = (
    df_tickets
    .filter((F.to_date(F.col("opened_at")) < F.lit("2025-02-18").cast("date")) & (F.col("requester_id") >= 5))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "channel",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
    )
)

with_windows = (
    recent
    .select(
        "id",
        "requester_id",
        "closed_at",
        F.rank().over(w_all_by_closed_at).alias("closed_at_rank"),
        F.count(F.lit(1)).over(w_channel).alias("rows_in_channel"),
    )
)

leaders = (
    with_windows
    .filter((F.col("closed_at_rank") == 1) & (F.month(F.col("closed_at")).isin(1, 2)))
    .drop("closed_at_rank")
)

enriched = (
    leaders
    .withColumn("requester_id_per_rows_in_channel", F.round(F.col("requester_id") / F.nullif(F.col("rows_in_channel"), F.lit(0)), 3))
)

result = (
    enriched
    .select(
        "id",
        "requester_id",
        "rows_in_channel",
        "requester_id_per_rows_in_channel",
    )
    .orderBy(F.col("requester_id").asc(), F.col("id").desc_nulls_first())
)
