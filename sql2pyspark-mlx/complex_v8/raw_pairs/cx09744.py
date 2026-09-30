from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_csat_filled_by_csat_fil = Window.partitionBy("csat", "csat_filled").orderBy(F.col("csat_filled").desc())
w_csat_csat_filled_by_csat_fil_2 = Window.partitionBy("csat", "csat_filled").orderBy(F.col("csat_filled").desc(), "id")
w_csat_csat_filled_by_csat_fil_3 = Window.partitionBy("csat", "csat_filled").orderBy(F.col("csat_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_tickets = (
    df_tickets
    .filter((F.month(F.col("closed_at")).isin(2, 3)) & (F.col("priority").isNotNull()))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "tags",
        F.when(F.col("requester_id") >= 16, "A").when(F.col("requester_id") >= 7, "B").otherwise("C").alias("requester_id_tier"),
    )
)

derived = (
    recent_tickets
    .select(
        "*",
        F.dayofweek(F.col("closed_at")).alias("closed_at_dayofweek"),
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
)

without_matches = (
    derived.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.col("t.closed_at") >= F.lit("2025-02-08 15:00:00").cast("timestamp")), "left_semi")
    .filter(F.col("csat") >= 2)
)

sequenced = (
    without_matches
    .select(
        "*",
        F.round(F.percent_rank().over(w_csat_csat_filled_by_csat_fil), 3).alias("csat_filled_percent_rank"),
        F.rank().over(w_csat_csat_filled_by_csat_fil_2).alias("csat_filled_rank"),
        F.nth_value("closed_at", 2).over(w_csat_csat_filled_by_csat_fil_3).alias("second_closed_at"),
    )
)

result = (
    sequenced
    .cube("csat", "requester_id_tier")
    .agg(
        F.grouping("csat").alias("is_total_csat"),
        F.round(F.avg("csat_filled_percent_rank"), 2).alias("avg_csat_filled_percent_rank"),
    )
    .select(
        "csat",
        F.coalesce(F.col("requester_id_tier"), F.lit("ALL")).alias("requester_id_tier"),
        "is_total_csat",
        "avg_csat_filled_percent_rank",
    )
)
