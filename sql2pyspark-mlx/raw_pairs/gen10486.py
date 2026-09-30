from pyspark.sql import functions as F

unpacked = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.coalesce(F.col("attributes")["device"], F.lit("direct")).alias("device_or_default"),
        F.col("ship_to.country").alias("ship_country"),
    )
)

result = (
    unpacked
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"), "customer_id")
    .agg(
        F.size(F.collect_set("ship_country")).alias("num_ship_countries"),
        F.min("created_at").alias("started_at"),
        F.size(F.collect_set("ship_country")).alias("num_ship_countries_2"),
    )
)
