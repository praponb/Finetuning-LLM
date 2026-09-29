from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("MONTH", "created_at")).orderBy(F.col("qty").desc_nulls_last())
w4 = Window.partitionBy("promo").orderBy(F.col("qty").desc())
w5 = Window.partitionBy("promo").orderBy(F.col("qty_sum_1h").desc_nulls_last())

exploded = (
    df_carts
    .withColumn("entry", F.explode_outer("items"))
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.col("entry.qty"),
        F.col("entry.price"),
        F.col("entry.promo"),
    )
)

with_windows = (
    exploded
    .filter(F.array_contains("coupon_codes", "SPRING10"))
    .withColumn("qty_share", F.round((F.col("qty") / F.sum("qty").over(w)), 4))
    .withColumn("qty_sum_1h", F.sum("qty").over(w2))
    .withColumn("qty_cume_dist", F.cume_dist().over(w3))
)

filtered = (
    with_windows
    .filter((F.col("created_at") >= F.lit("2025-03-02 00:00:00").cast("timestamp")) & F.col("promo").isNotNull())
    .withColumn("price_int", F.col("price").cast("int"))
)

with_windows_carts = (
    filtered
    .select(
        "created_at",
        "coupon_codes",
        "price",
        "price_int",
        F.rank().over(w4).alias("qty_rank"),
        F.stddev_samp("qty_sum_1h").over(w).alias("promo_stddev_samp_qty_sum_1h"),
        F.cume_dist().over(w5).alias("qty_sum_1h_cume_dist"),
    )
)

metrics = (
    with_windows_carts
    .select(
        "created_at",
        "coupon_codes",
        "qty_rank",
        "qty_sum_1h_cume_dist",
        F.sum("qty_sum_1h_cume_dist").over(w2).alias("qty_sum_1h_cume_dist_sum_1h"),
    )
)

result = (
    metrics
    .groupBy(F.date_trunc("hour", "created_at").alias("hour_window"))
    .agg(
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.max("created_at").alias("last_created_at"),
    )
    .orderBy(F.col("hour_window").desc_nulls_last())
)
