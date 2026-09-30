from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") == 224) & (F.col("event_type") != "search") & (F.col("duration_sec") < 184))
    .groupBy(
        F.when(F.col("duration_sec") >= 529, "high").when(F.col("duration_sec") >= 182, "mid").otherwise("low").alias("duration_sec_bucket"),
        "page",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
)
