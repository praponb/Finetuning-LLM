from pyspark.sql import functions as F

flattened_attrs = (
    df_carts
    .filter(F.col("created_at") >= "2023-09-01")
    .select(
        "id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.col("ship_to.zip").alias("ship_zip"),
        F.col("ship_to.city").alias("ship_city"),
        F.col("attributes")["device"].alias("device"),
        F.transform("items", lambda x: x["product_id"]).alias("product_ids"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("direct")).alias("utm_source_or_default"),
    )
)

result = (
    flattened_attrs
    .withColumn("hour_window", F.date_trunc("HOUR", "created_at"))
    .withColumn("activity_day", F.date_trunc("DAY", "created_at"))
    .groupingSets(
        [["hour_window", "activity_day"], ["hour_window"], []],
        "hour_window",
        "activity_day",
    )
    .agg(
        F.count(F.when(F.col("ship_city") == "Berlin", 1)).alias("berlin_rows"),
        F.count(F.when(F.col("ship_city") == "Singapore", 1)).alias("singapore_rows"),
        F.min("created_at").alias("first_created_at"),
        F.max("created_at").alias("last_seen"),
        F.size(F.collect_set("ship_city")).alias("num_ship_cities"),
        F.sort_array(F.collect_set("ship_city")).alias("ship_city_list"),
        F.grouping_id().alias("grouping_level"),
    )
)
