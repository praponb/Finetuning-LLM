from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.orderBy(F.col("price").desc_nulls_first())
w5 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w6 = Window.orderBy("price_max_1h_rounded")

lines = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
    )
)

scored = (
    lines
    .withColumn("price_max_1h", F.max("price").over(w))
    .withColumn("qty_max_2h", F.max("qty").over(w2))
    .withColumn("qty_sum_30m", F.sum("qty").over(w3))
)

scored_carts = (
    scored
    .select(
        "*",
        F.max("qty_sum_30m").over(w).alias("qty_sum_30m_max_1h"),
        F.avg("product_id").over(w3).alias("product_id_avg_30m"),
        F.dense_rank().over(w4).alias("price_dense_rank"),
    )
)

filtered = (
    scored_carts
    .filter(F.col("created_at") > F.lit("2025-03-02 02:00:00").cast("timestamp"))
    .select(
        "*",
        F.round("price_max_1h", 0).alias("price_max_1h_rounded"),
        F.round("qty_sum_30m_max_1h", 2).alias("qty_sum_30m_max_1h_rounded"),
    )
)

scored_carts_2 = (
    filtered
    .select(
        "*",
        F.max("qty_sum_30m_max_1h_rounded").over(w2).alias("qty_sum_30m_max_1h_rounded_max_2h"),
        F.sum("qty").over(w5).alias("qty_sum_15m"),
    )
)

valid = (
    scored_carts_2
    .filter((F.col("created_at") > F.lit("2025-03-02 13:00:00").cast("timestamp")) & (F.col("qty_sum_30m_max_1h") <= 10))
    .select(
        "*",
        (F.col("price_max_1h") - F.col("qty_sum_30m_max_1h_rounded")).alias("price_max_1h_minus_qty_sum_30m_max_1h_rounded"),
    )
)

matched_keys = (
    valid
    .select("id")
    .subtract(
        df_carts
        .filter(F.col("customer_id") == 40)
        .select("id")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("price_max_1h_rounded_cume_dist", F.cume_dist().over(w6))
    .withColumn("qty_sum_30m_avg_1h", F.avg("qty_sum_30m").over(w))
    .withColumn("qty_sum_15m_avg_2h", F.avg("qty_sum_15m").over(w2))
    .withColumn("price_max_1h_minus_qty_sum_30m_max_1h_rounded_sum_15m", F.sum("price_max_1h_minus_qty_sum_30m_max_1h_rounded").over(w5))
)
