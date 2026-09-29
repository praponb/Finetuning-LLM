from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("agent_id").orderBy(F.col("closed_at").desc(), F.col("id").desc())

base = (
    df_tickets
    .filter((F.hour(F.col("closed_at")).between(9, 15)) & (F.col("opened_at") < F.lit("2025-02-06 15:00:00").cast("timestamp")))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        F.abs(F.col("csat") - 4).alias("csat_distance"),
        F.round((F.unix_timestamp(F.col("opened_at")) - F.unix_timestamp(F.col("closed_at"))) / 60, 1).alias("minutes_closed_at_to_opened_at"),
    )
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

without_matches = (
    latest
    .crossJoin(latest.agg(F.max("csat_distance").alias("overall_max_csat_distance")))
    .filter(F.col("csat_distance") < F.col("overall_max_csat_distance"))
    .drop("overall_max_csat_distance")
)

screened = (
    without_matches.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.month(F.col("t.opened_at")).isin(1, 3)), "left_anti")
)

result = (
    screened
    .groupingSets(
        [["channel", "agent_id", "csat"], ["channel"], []],
        "channel", "agent_id", "csat",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.max("csat_distance").alias("max_csat_distance"),
        F.percentile_approx("csat_distance", 0.5).alias("median_csat_distance"),
    )
)
