from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        F.exists("items", lambda li: li["price"] > 100).alias("has_premium_item"),
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.coalesce(F.col("attributes")["gift"], F.lit("none")).alias("gift_or_default"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("direct")).alias("utm_source_or_default"),
        F.coalesce(F.col("ship_to.city"), F.lit("UNKNOWN")).alias("city_or_unknown"),
    )
)

result = (
    parsed
    .cube("customer_id")
    .agg(
        F.countDistinct("gift_or_default").alias("distinct_gift_or_defaults"),
        F.count("*").alias("event_count"),
        F.count("*").alias("cnt"),
        F.countDistinct("gift_or_default").alias("distinct_gift_or_defaults_2"),
        F.countDistinct("utm_source").alias("distinct_utm_sources"),
        F.grouping("customer_id").alias("grouping_level"),
    )
    .filter(F.col("distinct_gift_or_defaults") >= 1)
    .orderBy(F.col("cnt").desc())
)
