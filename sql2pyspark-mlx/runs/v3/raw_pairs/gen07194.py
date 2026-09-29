from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2023-12-01")
    .groupBy(
        "session_id",
        F.when(F.col("page").isin(["/home", "/product"]), "core").otherwise("other").alias("page_group"),
    )
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .filter(F.col("total_duration_sec") < 2200)
    .orderBy("session_id")
)
