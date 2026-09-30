from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "attributes",
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
    )
)

result = (
    unpacked
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("event_count_2"),
        F.countDistinct("city_or_unknown").alias("distinct_city_or_unknowns"),
        F.countDistinct("utm_source").alias("distinct_utm_sources"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("event_count_2") < 1000)
    .orderBy(F.col("event_count").desc())
)
