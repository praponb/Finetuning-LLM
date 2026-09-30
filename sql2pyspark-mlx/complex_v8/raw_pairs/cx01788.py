from pyspark.sql import functions as F

filtered_tickets = (
    df_tickets
    .filter(F.col("channel") != "email")
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("requester_id"), F.lit(0)).alias("requester_id_filled"),
        (F.col("requester_id") + F.col("csat")).alias("requester_id_plus_csat"),
    )
)

derived = (
    filtered_tickets
    .filter(F.col("csat") < 4)
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
        "requester_id_filled",
        "requester_id_plus_csat",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
        F.round(F.col("csat") / F.nullif(F.col("requester_id_plus_csat"), F.lit(0)), 3).alias("csat_per_requester_id_plus_csat"),
    )
)

enriched = (
    derived
    .filter(F.col("priority") == "P1")
    .select(
        "*",
        (F.col("requester_id_filled") - F.col("requester_id_plus_csat")).alias("requester_id_filled_minus_requester_id_plus_csat"),
        F.when(F.col("priority").isin("P2", "P3"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other").alias("priority_group"),
    )
)

scored = enriched.withColumn("closed_at_plus_1d", F.col("closed_at") + F.expr("INTERVAL 1 DAYS"))

result = (
    scored
    .select("id", "csat_filled", "requester_id_plus_csat")
    .unpivot(
        ids=["id"],
        values=["csat_filled", "requester_id_plus_csat"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
