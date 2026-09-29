from pyspark.sql import functions as F

cleaned_tickets = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
        "tags",
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
    )
)

scored = (
    cleaned_tickets
    .filter(F.size(F.col("tags")) >= 1)
    .withColumn("csat_tier", F.when(F.col("csat") >= 4, "HIGH").when(F.col("csat") >= 3, "MEDIUM").otherwise("LOW"))
)

totals = (
    scored
    .groupBy("csat")
    .agg(F.count("*").alias("n_rows"), F.sum("csat").alias("total_csat"))
)

result = (
    totals
    .select(
        "csat",
        "n_rows",
        "total_csat",
        F.when(F.col("csat") >= 4, "HIGH").when(F.col("csat") >= 2, "MEDIUM").otherwise("LOW").alias("csat_tier"),
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
    .orderBy(F.col("csat_tier").desc(), "total_csat")
)
