from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("page").isin(["/cart", "/home"]), "core").otherwise("other").alias("page_group"),
    )
    .agg(
        F.sum(F.when(F.col("event_type") == "click", F.col("duration_sec")).otherwise(0)).alias("click_duration_sec"),
    )
    .filter(F.col("click_duration_sec") < 77)
    .select("page_group", "click_duration_sec")
)
