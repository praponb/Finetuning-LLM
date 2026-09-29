from pyspark.sql import functions as F

lines = (
    df_carts
    .select("id", "created_at", F.explode("attributes").alias("attr_key", "value"))
)

result = (
    lines
    .groupBy("attr_key")
    .agg(
        F.max("created_at").alias("last_seen"),
        F.countDistinct("attr_key").alias("distinct_attr_keys"),
        F.count("*").alias("event_count"),
        F.count("*").alias("row_count"),
    )
)
