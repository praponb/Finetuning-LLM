from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

result = (
    exploded
    .groupBy("attr_key", "customer_id")
    .agg(
        F.max("created_at").alias("last_seen"),
        F.count_if(F.col("attr_key") == "utm_source").alias("utm_source_events"),
        F.max("created_at").alias("last_created_at"),
        F.count_if(F.col("attr_key") == "device").alias("device_events"),
    )
)
