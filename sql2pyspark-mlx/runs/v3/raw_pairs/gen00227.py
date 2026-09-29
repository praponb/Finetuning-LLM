from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(125, 187))
    .select(
        "event_type",
        "page",
        "duration_sec",
        F.when(F.col("page").isin(["/home", "/cart"]), "Core").otherwise("Other").alias("page_group"),
    )
)
