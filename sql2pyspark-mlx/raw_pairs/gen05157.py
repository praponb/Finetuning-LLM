from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").desc())
w2 = Window.orderBy("price")
w3 = Window.orderBy(F.col("qty").desc_nulls_first())
w4 = Window.orderBy("price_rank_2")
w5 = Window.orderBy(F.col("greatest_price_rank_2_price_rank").asc_nulls_first())

exploded = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
    )
)

metrics = (
    exploded
    .filter(F.col("product_id") >= 6)
    .select(
        "id",
        "price",
        F.rank().over(w).alias("price_rank"),
        F.rank().over(w2).alias("price_rank_2"),
        F.percent_rank().over(w3).alias("qty_percent_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("qty_percent_rank") > 0.43)
    .select(
        "price_rank",
        "price_rank_2",
        (F.col("qty_percent_rank") - F.col("price_rank")).alias("qty_percent_rank_minus_price_rank"),
    )
)

filtered = (
    valid
    .filter(F.col("qty_percent_rank_minus_price_rank") < 31)
    .withColumn("greatest_price_rank_2_price_rank", F.greatest("price_rank_2", "price_rank"))
)

base = (
    filtered
    .filter(F.col("price_rank") < 6)
    .withColumn("greatest_price_rank_2_price_rank_2", F.greatest("price_rank_2", "price_rank"))
)

enriched = (
    base
    .select(
        "price_rank",
        "price_rank_2",
        "greatest_price_rank_2_price_rank",
        F.rank().over(w4).alias("price_rank_2_rank"),
        F.cume_dist().over(w5).alias("greatest_price_rank_2_price_rank_cume_dist"),
        F.percent_rank().over(w5).alias("greatest_price_rank_2_price_rank_percent_rank"),
    )
)

totals = (
    enriched
    .groupBy(
        F.when(F.col("greatest_price_rank_2_price_rank") >= 88, "LARGE").otherwise("SMALL").alias("greatest_price_rank_2_price_rank_tier"),
    )
    .agg(
        F.percentile_approx("price_rank_2_rank", [0.25, 0.5, 0.75]).alias("price_rank_2_rank_quartiles"),
        F.sum("price_rank_2").alias("total_price_rank_2"),
        F.min("greatest_price_rank_2_price_rank").alias("min_greatest_price_rank_2_price_rank"),
    )
)

result = (
    totals
    .select(
        "greatest_price_rank_2_price_rank_tier",
        "price_rank_2_rank_quartiles",
        "min_greatest_price_rank_2_price_rank",
    )
    .orderBy("greatest_price_rank_2_price_rank_tier")
)
