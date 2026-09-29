from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_part = Window.partitionBy("key", "customer_id").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("age").desc_nulls_first())
w_running_2 = Window.orderBy(F.col("age_scaled").desc_nulls_first())
w5 = Window.partitionBy("key")
w6 = Window.partitionBy(F.month("created_at"), "key").orderBy(F.col("age_scaled").asc_nulls_first())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "value"),
    )
)

enriched = (
    exploded
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.age"), F.col("p.email"), F.col("p.name"))
)

filtered = (
    enriched
    .filter(F.col("key").isNotNull())
    .withColumn("name_normalized", F.upper(F.trim("name")))
    .withColumn("is_welcome5", F.array_contains("coupon_codes", "WELCOME5"))
    .withColumn("key_filled", F.coalesce("key", F.lit("DIRECT")))
)

cleaned = (
    filtered
    .filter(F.col("key").isNotNull() & (F.col("created_at") > F.lit("2025-03-01").cast("date")))
    .select(
        "*",
        F.round((F.col("age") * 100), 2).alias("age_scaled"),
        F.coalesce("key", F.lit("N/A")).alias("resolved_key"),
    )
)

metrics = (
    cleaned
    .select(
        "created_at",
        "key",
        "value",
        "age",
        "email",
        "name",
        "key_filled",
        "age_scaled",
        "resolved_key",
        F.sum("age").over(w_running).alias("age_sum_15m"),
        F.count("age").over(w_part).alias("age_count_2h"),
        F.dense_rank().over(w3).alias("age_dense_rank"),
        F.percent_rank().over(w_running_2).alias("age_scaled_percent_rank"),
    )
)

with_windows = (
    metrics
    .withColumn("key_min_age", F.min("age").over(w5))
    .withColumn("age_dense_rank_share", F.round((F.col("age_dense_rank") / F.sum("age_dense_rank").over(w5)), 4))
    .withColumn("age_scaled_dense_rank", F.dense_rank().over(w6))
)

result = (
    with_windows
    .groupBy("key", F.date_trunc("hour", "created_at").alias("hour_window"))
    .agg(
        F.max(F.when(F.col("key") == "gift", F.col("key_min_age"))).alias("gift_max_key_min_age"),
        F.min("created_at").alias("first_seen"),
        F.min("created_at").alias("first_created_at"),
    )
)
