from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("price").desc_nulls_first())
w2 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w3 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w4 = Window.partitionBy("customer_id", "promo").orderBy(F.col("price").desc_nulls_last())
w5 = Window.partitionBy(F.date_trunc("MONTH", "created_at"), "promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w6 = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)

flattened = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "line"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        (F.col("pos") + 1).alias("line_no"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

with_windows = flattened.withColumn("price_dense_rank", F.dense_rank().over(w))

valid = (
    with_windows
    .filter((F.col("created_at") > F.lit("2025-03-01 19:00:00").cast("timestamp")) & (~F.col("promo").eqNullSafe(F.lit("WELCOME5"))))
    .select("*", F.sort_array("coupon_codes").alias("sorted_coupon_codes"))
)

filtered = (
    valid
    .filter((F.col("promo").eqNullSafe(F.lit("BUNDLE"))) & (F.col("created_at") > F.lit("2025-03-01").cast("date")))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "line_no",
        "price",
        "promo",
        "price_dense_rank",
        "sorted_coupon_codes",
        F.sort_array("coupon_codes").alias("sorted_coupon_codes_2"),
        (F.col("price") / F.nullif("line_no", F.lit(0))).alias("price_per_line_no"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "price",
        "price_dense_rank",
        "sorted_coupon_codes",
        "sorted_coupon_codes_2",
        "price_per_line_no",
        F.count("line_no").over(w2).alias("line_no_count_2h"),
        F.max("price_per_line_no").over(w3).alias("price_per_line_no_max_30m"),
        F.dense_rank().over(w4).alias("price_dense_rank_2"),
        F.sum("line_no").over(w5).alias("line_no_sum_30m"),
        F.max("price_dense_rank").over(w6).alias("price_dense_rank_max_1h"),
    )
)

result = (
    enriched
    .select(
        "id",
        "created_at",
        "price",
        "price_dense_rank",
        "sorted_coupon_codes",
        "sorted_coupon_codes_2",
        "price_per_line_no",
        "price_per_line_no_max_30m",
        "price_dense_rank_2",
    )
    .orderBy(F.col("price_dense_rank_2").desc(), F.col("price_dense_rank").desc())
)
