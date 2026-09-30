from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("created_at").desc(), "id", "key")
w2 = Window.partitionBy("customer_id", "key").orderBy("created_at", "id", "key")

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("key", "value"),
    )
)

valid = (
    lines
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & (~F.col("key").eqNullSafe(F.lit("device"))))
    .select(
        "id",
        "customer_id",
        "created_at",
        "key",
        "value",
        F.coalesce("key", F.lit("Unknown")).alias("key_filled"),
        F.coalesce("key", F.lit("NONE")).alias("key_filled_2"),
    )
)

ranked = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest = (
    ranked
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

enriched = (
    latest
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        "s.id",
        "s.customer_id",
        "s.key",
        "s.value",
        "s.key_filled_2",
        F.col("c.segment"),
        F.col("c.city"),
    )
)

result = (
    enriched
    .rollup("segment", "city")
    .agg(
        F.count_if(F.col("city") == "Singapore").alias("singapore_events"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("key") == "utm_source", 1)).alias("utm_source_count"),
        F.count(F.when(F.col("key") == "utm_source", 1)).alias("utm_source_count_2"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("utm_source_count_2") > 1)
)
