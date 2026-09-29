from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo")
w2 = Window.orderBy(F.col("product_id_filled").desc())

flattened = (
    df_carts
    .withColumn("item", F.explode("items"))
    .select(
        "id",
        "customer_id",
        F.col("item.product_id"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

valid = (
    flattened
    .filter(F.col("customer_id") == 10)
    .select(
        "id",
        "product_id",
        "promo",
        F.col("price").cast("int").alias("price_int"),
        F.coalesce("promo", F.lit("N/A")).alias("promo_clean"),
    )
)

filtered = (
    valid
    .filter(F.col("price_int").between(33, 42))
    .select(
        "*",
        F.substring("promo", 1, 2).alias("promo_prefix"),
        F.coalesce("promo", F.lit("NONE")).alias("resolved_promo"),
    )
)

filtered_carts = (
    filtered
    .filter((~F.col("promo").eqNullSafe(F.lit("VIP"))) & (F.col("price_int") <= 31))
    .select(
        "id",
        "product_id",
        "promo",
        "promo_clean",
        "promo_prefix",
        "resolved_promo",
        F.coalesce("product_id", F.lit(0)).alias("product_id_filled"),
    )
)

cleaned = (
    filtered_carts
    .filter(F.col("promo").isNotNull() & (F.col("product_id") < 9))
    .select(
        "id",
        "product_id",
        "promo",
        "promo_clean",
        "resolved_promo",
        "product_id_filled",
        (F.col("product_id") * F.col("product_id_filled")).alias("product_id_times_product_id_filled"),
    )
)

enriched = (
    cleaned
    .filter(F.col("product_id_times_product_id_filled") > 54)
    .select(
        "id",
        "product_id",
        "promo_clean",
        "product_id_times_product_id_filled",
        F.stddev_samp("product_id_times_product_id_filled").over(w).alias("promo_stddev_samp_product_id_times_product_id_filled"),
        F.stddev_samp("product_id_filled").over(w).alias("promo_stddev_samp_product_id_filled"),
        F.rank().over(w2).alias("product_id_filled_rank"),
        F.sum("product_id_times_product_id_filled").over(w).alias("promo_sum_product_id_times_product_id_filled"),
    )
)

result = (
    enriched
    .groupBy("product_id")
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("promo_stddev_samp_product_id_filled", [0.25, 0.5, 0.75]).alias("promo_stddev_samp_product_id_filled_quartiles"),
        F.percentile_approx("product_id_filled_rank", 0.9).alias("product_id_filled_rank_p90"),
    )
)
