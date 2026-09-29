from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type") != "page_view") & F.col("page").isin(["/cart", "/checkout", "/product"]))
    .groupBy(
        F.when(F.col("page").isin(["/search", "/product"]), "tier_1").otherwise("other").alias("page_group"),
        F.date_trunc("MONTH", "event_time").alias("event_month"),
    )
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy(F.col("page_group").desc())
)
