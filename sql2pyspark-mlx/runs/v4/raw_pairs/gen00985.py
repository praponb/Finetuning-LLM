from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .groupBy("key")
    .agg(
        F.count(F.when(F.col("key") == "utm_source", 1)).alias("utm_source_rows"),
        F.countDistinct("key").alias("distinct_keys"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.count("*").alias("cnt"),
        F.min("created_at").alias("first_created_at"),
        F.count_if(F.col("key") == "utm_source").alias("utm_source_events"),
    )
)
