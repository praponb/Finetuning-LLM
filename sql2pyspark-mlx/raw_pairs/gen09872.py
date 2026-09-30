from pyspark.sql import functions as F

base = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-07-03").cast("date")) & F.col("page").isNotNull())
    .select(
        "*",
        F.when(F.col("page").isin(["/home", "/cart"]), "Tier 1").otherwise("Other").alias("page_group"),
        F.coalesce("page", F.lit("OTHER")).alias("page_clean"),
    )
)

result = (
    base
    .groupBy("page", "session_id")
    .pivot("event_type", ["search", "page_view"])
    .agg(F.sum("duration_sec").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
