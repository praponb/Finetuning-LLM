from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("created_at"), "element").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.partitionBy("element")
w3 = Window.partitionBy("element").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.partitionBy("element").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w5 = Window.partitionBy("element").orderBy(F.col("qty_count_2h").desc())

exploded = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

lines = (
    exploded
    .select(
        "id",
        "created_at",
        "product_id",
        "qty",
        F.explode("coupon_codes").alias("element"),
    )
)

with_windows = (
    lines
    .select(
        "id",
        "created_at",
        "product_id",
        "qty",
        "element",
        F.max("qty").over(w).alias("qty_max_1h"),
        F.round((F.col("qty") / F.sum("qty").over(w2)), 4).alias("qty_share"),
        F.count("qty").over(w3).alias("qty_count_2h"),
    )
)

scored = (
    with_windows
    .filter(F.col("created_at") < "2025-06-19")
    .withColumn("element_min_qty_share", F.min("qty_share").over(w2))
    .withColumn("product_id_avg_15m", F.avg("product_id").over(w4))
    .withColumn("qty_count_2h_dense_rank", F.dense_rank().over(w5))
    .withColumn("qty_max_1h_share", F.round((F.col("qty_max_1h") / F.sum("qty_max_1h").over(w2)), 4))
)

cleaned = (
    scored
    .filter((F.col("created_at") >= F.lit("2025-03-01").cast("date")) & F.col("element").isNotNull())
    .select(
        "*",
        (F.col("product_id_avg_15m") * F.col("qty_max_1h")).alias("product_id_avg_15m_times_qty_max_1h"),
    )
)

base = (
    cleaned
    .filter(~F.col("element").eqNullSafe(F.lit("BUNDLE")))
    .select("*", F.upper("element").alias("element_upper"))
)

base_carts = (
    base
    .filter((F.col("created_at") > F.lit("2025-03-02 00:00:00").cast("timestamp")) & F.col("qty_max_1h").isNotNull())
    .select("*", F.weekofyear("created_at").alias("created_at_weekofyear"))
)

result = (
    base_carts
    .select(
        "product_id",
        "qty",
        "qty_max_1h",
        "qty_share",
        "element_min_qty_share",
        "product_id_avg_15m",
        "qty_count_2h_dense_rank",
        "qty_max_1h_share",
        "product_id_avg_15m_times_qty_max_1h",
        "element_upper",
        "created_at_weekofyear",
        F.stddev_samp("qty").over(w2).alias("element_stddev_samp_qty"),
    )
)
