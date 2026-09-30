from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("qty").desc())
w2 = Window.orderBy(F.col("qty_whole_minus_price").asc_nulls_last())

lines = (
    df_carts
    .withColumn("line", F.explode_outer("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

base = (
    lines
    .filter(F.col("promo").isin(["WELCOME5", "VIP", "SPRING10"]))
    .select("id", "coupon_codes", "qty", "price", F.ceil("qty").alias("qty_whole"))
)

base_carts = (
    base
    .filter(F.col("qty") != 3)
    .select(
        "id",
        "qty",
        "qty_whole",
        (F.col("qty_whole") - F.col("price")).alias("qty_whole_minus_price"),
        F.col("qty_whole").cast("string").alias("qty_whole_num"),
    )
)

base_carts_2 = (
    base_carts
    .filter(F.col("qty").isNotNull())
    .select(
        "qty",
        "qty_whole_minus_price",
        "qty_whole_num",
        F.coalesce("qty", F.lit(0)).alias("qty_filled"),
    )
)

metrics = (
    base_carts_2
    .filter(F.col("qty") == 4)
    .select(
        "qty",
        "qty_whole_minus_price",
        F.dense_rank().over(w).alias("qty_dense_rank"),
        F.rank().over(w2).alias("qty_whole_minus_price_rank"),
    )
)

cleaned = (
    metrics
    .filter(F.col("qty").between(3, 4))
    .withColumn("qty_whole_minus_price_filled", F.coalesce("qty_whole_minus_price", F.lit(0)))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("qty") >= 5, "HIGH").otherwise("LOW").alias("qty_tier"),
        F.when(F.col("qty_whole_minus_price_filled") >= 71, "HIGH").otherwise("LOW").alias("qty_whole_minus_price_filled_tier"),
        F.when(F.col("qty_whole_minus_price_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("qty_whole_minus_price_rank_tier"),
    )
    .agg(
        F.max("qty_dense_rank").alias("max_qty_dense_rank"),
        F.round(F.avg("qty_dense_rank"), 2).alias("avg_qty_dense_rank"),
        F.round(F.avg("qty_whole_minus_price"), 2).alias("avg_qty_whole_minus_price"),
        F.round(F.avg("qty_whole_minus_price_filled"), 2).alias("avg_qty_whole_minus_price_filled"),
    )
    .orderBy(F.col("max_qty_dense_rank").desc())
)
