from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 540, "high").when(F.col("duration_sec") >= 185, "mid").otherwise("low").alias("duration_sec_bucket"),
        "page",
        "event_type",
    )
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"))
    .orderBy("page_values")
)
