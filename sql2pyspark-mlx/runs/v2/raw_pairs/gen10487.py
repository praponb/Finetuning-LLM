from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isin(["/home", "/cart"]))
    .select(
        "id",
        "session_id",
        "event_time",
        F.when(F.col("page").isin(["/product", "/checkout"]), "Tier 1").otherwise("Other").alias("page_group"),
        F.length("page").alias("page_length"),
    )
    .orderBy("event_time", "id")
    .limit(10)
)
