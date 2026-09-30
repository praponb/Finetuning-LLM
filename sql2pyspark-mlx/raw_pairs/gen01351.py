from pyspark.sql import functions as F

unpacked = (
    df_carts
    .filter(F.hour("created_at").between(9, 13))
    .select(
        "id",
        "created_at",
        "ship_to",
        "attributes",
        F.exists("items", lambda li: li["price"] > 100).alias("has_premium_item"),
        F.col("ship_to.country").alias("ship_country"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    unpacked
    .groupBy("ship_city")
    .agg(
        F.count_if(F.col("ship_city") == "Chiang Mai").alias("chiang_mai_events"),
        F.count("*").alias("cnt"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("chiang_mai_events").desc_nulls_first())
)
