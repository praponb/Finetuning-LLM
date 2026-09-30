from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2025-08-14")
    .select(
        "id",
        "event_type",
        "event_time",
        F.trim("event_type").alias("clean_event_type"),
        F.when(F.col("page").isin(["/checkout", "/product"]), "Core").otherwise("Other").alias("page_group"),
    )
    .orderBy("page_group", "id")
    .limit(10)
)
