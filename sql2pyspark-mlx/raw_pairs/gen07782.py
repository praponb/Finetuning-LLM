from pyspark.sql import functions as F

result = (
    df_web_events
    .rollup(
        F.when(F.col("event_type").isin(["click", "add_to_cart"]), "priority").otherwise("other").alias("event_type_group"),
        "event_type",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
    .orderBy(F.col("event_type").desc())
)
