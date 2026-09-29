from pyspark.sql import functions as F

valid_tickets = (
    df_tickets
    .filter((F.col("requester_id") < 15) | (F.col("csat") <= 4))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        "tags",
    )
)

rollup_base = (
    valid_tickets
    .filter(F.col("agent_id") <= 8)
    .groupBy("csat")
    .agg(
        F.round(F.sum("requester_id") / F.nullif(F.sum("csat"), F.lit(0)), 3).alias("requester_id_to_csat_ratio"),
        F.min("closed_at").alias("first_closed_at"),
        F.percentile_approx("csat", 0.5).alias("median_csat"),
        F.max("csat").alias("max_csat_check"),
    )
    .filter(F.col("max_csat_check") > 3)
    .drop("max_csat_check")
)

labeled = rollup_base.withColumn("csat_distance", F.abs(F.col("csat") - 3))

result = (
    labeled
    .filter(F.col("csat") < 5)
    .select(
        "csat",
        "first_closed_at",
        "csat_distance",
        F.coalesce(F.col("csat_distance"), F.lit(0)).alias("csat_distance_filled"),
        F.date_format(F.col("first_closed_at"), "MMM yyyy").alias("first_closed_at_label"),
    )
    .orderBy(F.col("csat").desc(), F.col("first_closed_at").desc_nulls_first())
)
