from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.orderBy(F.col("price_max_2h_whole").desc_nulls_last())
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_ordered = Window.partitionBy(F.date_trunc("month", "created_at")).orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w5 = Window.orderBy(F.col("adjusted_price_max_2h").desc_nulls_last())
w6 = Window.partitionBy(F.to_date("created_at")).orderBy(F.col("adjusted_price_max_2h").asc_nulls_first())
w7 = Window.orderBy(F.col("adjusted_price_max_2h").desc_nulls_first())
w8 = Window.partitionBy(F.month("created_at")).orderBy(F.col("adjusted_price_max_2h_sum_1h").desc_nulls_first())
w9 = Window.orderBy(F.col("price_max_2h").asc_nulls_first())

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
        F.col("line.promo"),
    )
)

flattened = (
    lines
    .select(
        "id",
        "created_at",
        "qty",
        "price",
        "promo",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

with_windows = (
    flattened
    .filter(F.col("created_at") < "2025-07-01")
    .select("id", "created_at", F.max("price").over(w).alias("price_max_2h"))
)

filtered = (
    with_windows
    .filter(F.col("created_at") >= F.lit("2025-03-01 20:00:00").cast("timestamp"))
    .withColumn("price_max_2h_whole", F.floor("price_max_2h"))
    .withColumn("adjusted_price_max_2h", F.round((F.col("price_max_2h") * 1.07), 2))
)

with_windows_carts = (
    filtered
    .select(
        "id",
        "created_at",
        "price_max_2h",
        "price_max_2h_whole",
        "adjusted_price_max_2h",
        F.rank().over(w2).alias("price_max_2h_whole_rank"),
        F.max("price_max_2h").over(w3).alias("price_max_2h_max_15m"),
        F.sum("adjusted_price_max_2h").over(w_ordered).alias("adjusted_price_max_2h_sum_1h"),
    )
)

enriched = (
    with_windows_carts
    .select(
        "id",
        "price_max_2h_whole",
        "adjusted_price_max_2h",
        "price_max_2h_max_15m",
        "adjusted_price_max_2h_sum_1h",
        F.cume_dist().over(w5).alias("adjusted_price_max_2h_cume_dist"),
        F.rank().over(w6).alias("adjusted_price_max_2h_rank"),
        F.dense_rank().over(w7).alias("adjusted_price_max_2h_dense_rank"),
        F.dense_rank().over(w8).alias("adjusted_price_max_2h_sum_1h_dense_rank"),
        F.cume_dist().over(w9).alias("price_max_2h_cume_dist"),
    )
)

filtered_carts = (
    enriched
    .filter(F.col("adjusted_price_max_2h_cume_dist").isNotNull())
    .withColumn("adjusted_price_max_2h_int", F.col("adjusted_price_max_2h").cast("int"))
    .withColumn("price_max_2h_cume_dist_int", F.col("price_max_2h_cume_dist").cast("int"))
)

result = (
    filtered_carts
    .groupBy(
        F.when(F.col("adjusted_price_max_2h_int") >= 75, "PREMIUM").otherwise("STANDARD").alias("adjusted_price_max_2h_int_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("adjusted_price_max_2h_rank", [0.25, 0.5, 0.75]).alias("adjusted_price_max_2h_rank_quartiles"),
        F.max("adjusted_price_max_2h_sum_1h_dense_rank").alias("max_adjusted_price_max_2h_sum_1h_dense_rank"),
    )
    .filter(F.col("max_adjusted_price_max_2h_sum_1h_dense_rank") > 0)
    .orderBy(F.col("adjusted_price_max_2h_int_tier").desc_nulls_last())
)
