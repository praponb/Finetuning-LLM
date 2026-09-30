from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") < "2023-10-01") & ~F.col("page").isin(["/product", "/checkout", "/cart"]))
    .groupBy(
        F.when(F.col("event_type").isin(["click", "purchase"]), "core").otherwise("other").alias("event_type_group"),
        "event_type",
    )
    .agg(F.max("event_time").alias("last_event_time"))
)
