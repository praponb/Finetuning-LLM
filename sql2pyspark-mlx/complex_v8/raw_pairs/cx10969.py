from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("channel", "requester_id_tier").orderBy(F.col("least_csat_requester_id").desc(), F.col("id").desc())

valid = (
    df_tickets
    .filter((F.col("agent_id").isNotNull()) & (F.col("channel") != "phone"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        F.when(F.col("requester_id") >= 22, "HIGH").when(F.col("requester_id") >= 7, "MEDIUM").otherwise("LOW").alias("requester_id_tier"),
        F.least(F.col("csat"), F.col("requester_id")).alias("least_csat_requester_id"),
    )
)

latest_per_key = (
    valid
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .groupingSets(
        [["agent_id", "channel", "requester_id_tier"], ["agent_id"], ["channel"], []],
        "agent_id", "channel", "requester_id_tier",
    )
    .agg(
        F.sum("csat").alias("total_csat"),
        F.percentile_approx("least_csat_requester_id", 0.5).alias("median_least_csat_requester_id"),
    )
    .select(
        "agent_id",
        F.coalesce(F.col("channel"), F.lit("ALL")).alias("channel"),
        F.coalesce(F.col("requester_id_tier"), F.lit("ALL")).alias("requester_id_tier"),
        "total_csat",
        "median_least_csat_requester_id",
    )
    .orderBy(
        F.col("agent_id").asc_nulls_last(),
        F.col("channel").asc_nulls_last(),
        F.col("requester_id_tier").asc_nulls_last(),
    )
)
