from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("segment").orderBy(F.col("age").desc_nulls_last())
w3 = Window.partitionBy("segment").orderBy(F.col("age").desc_nulls_last(), "id")

filtered = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.concat("segment", F.lit(" "), "city").alias("segment_city"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.city",
        "s.segment",
        "s.is_active",
        "s.credit_limit",
        "s.age",
        "s.segment_city",
        F.col("c.country").alias("session_country"),
    )
)

deduped = (
    with_last
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

with_last_customers = (
    deduped
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn_2", F.row_number().over(w))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.id",
        "s.email",
        "s.city",
        "s.segment",
        "s.age",
        "s.segment_city",
        "s.rn",
        F.col("c.device").alias("session_device"),
    )
)

ranked = (
    with_last_customers
    .withColumn("rn_2", F.row_number().over(w3))
    .filter(F.col("rn_2") == 1)
)

by_type = (
    ranked
    .groupBy("session_device")
    .pivot("city", ["Berlin", "Singapore"])
    .agg(F.sum("age").alias("value_sum"), F.count(F.lit(1)).alias("item_count"))
    .select(
        "session_device",
        F.col("Berlin_value_sum").alias("berlin_value_sum"),
        F.col("Berlin_item_count").alias("berlin_item_count"),
        F.col("Singapore_value_sum").alias("singapore_value_sum"),
        F.col("Singapore_item_count").alias("singapore_item_count"),
    )
)

result = (
    by_type
    .select(
        "session_device",
        "berlin_value_sum",
        "singapore_value_sum",
        "singapore_item_count",
    )
    .orderBy(F.col("singapore_item_count").desc(), "session_device")
)
