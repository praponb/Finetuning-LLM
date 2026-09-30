from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("promo")
w3 = Window.partitionBy("value")

lines = df_carts.select("id", "created_at", "coupon_codes", F.inline("items"))

lines_carts = (
    lines
    .select(
        "id",
        "created_at",
        "price",
        "promo",
        F.explode_outer("coupon_codes").alias("value"),
    )
)

base = (
    lines_carts
    .filter((F.col("created_at") > F.lit("2025-03-01").cast("date")) & F.col("promo").isNotNull())
    .withColumn("price_adjusted", F.round((F.col("price") * 1.15), 2))
    .withColumn("value_filled", F.coalesce("value", F.lit("NONE")))
)

base_carts = (
    base
    .filter(F.col("created_at") > F.lit("2025-03-02 11:00:00").cast("timestamp"))
    .select(
        "id",
        "created_at",
        "price",
        "promo",
        "value",
        "price_adjusted",
        "value_filled",
        F.upper("promo").alias("promo_upper"),
        (F.col("price") / F.nullif("price_adjusted", F.lit(0))).alias("price_per_price_adjusted"),
    )
)

metrics = (
    base_carts
    .select(
        "*",
        F.count("price_adjusted").over(w).alias("price_adjusted_count_1h"),
        F.round((F.col("price") / F.sum("price").over(w2)), 4).alias("price_share"),
        F.round((F.col("price") / F.sum("price").over(w3)), 4).alias("price_share_2"),
    )
)

result = (
    metrics
    .groupBy(
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        F.when(F.col("price_adjusted_count_1h") >= 16, "HIGH").otherwise("LOW").alias("price_adjusted_count_1h_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("value")).alias("value_list"),
        F.size(F.collect_set("value")).alias("num_values"),
        F.min("created_at").alias("first_seen"),
    )
    .orderBy(F.col("hour_window").asc_nulls_last())
)
