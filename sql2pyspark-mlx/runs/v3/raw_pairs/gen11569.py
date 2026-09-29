from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "event_type",
        "session_id",
        F.when(F.col("page").isin(["/cart", "/product"]), "Priority").otherwise("Other").alias("page_group"),
    )
    .orderBy(F.col("id").desc())
)
