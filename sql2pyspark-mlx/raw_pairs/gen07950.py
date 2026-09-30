from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("unit_price").asc_nulls_last())
w2 = Window.orderBy(F.col("unit_price").desc_nulls_first())
w3 = Window.partitionBy("order_id", "product_id")
w4 = Window.partitionBy("order_id")
w5 = Window.orderBy(F.col("unit_price_rank").asc_nulls_first())
w6 = Window.partitionBy("product_id")
w7 = Window.partitionBy("product_id").orderBy(F.col("unit_price_rank_cume_dist").desc())

remaining_keys = (
    df_order_items
    .select("order_id", "product_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([18, 30]))
        .select("order_id", "product_id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.rank().over(w).alias("unit_price_rank"),
    )
)

metrics_order_items = (
    metrics
    .filter(F.col("unit_price_rank") <= 3)
    .select(
        "product_id",
        "quantity",
        "unit_price",
        F.percent_rank().over(w2).alias("unit_price_percent_rank"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4).alias("unit_price_share"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w4)), 4).alias("unit_price_share_2"),
        F.cume_dist().over(w5).alias("unit_price_rank_cume_dist"),
        F.min("unit_price_rank").over(w4).alias("order_id_min_unit_price_rank"),
    )
)

scored = (
    metrics_order_items
    .select(
        "product_id",
        "unit_price",
        "unit_price_share",
        "unit_price_share_2",
        F.round((F.col("quantity") / F.sum("quantity").over(w6)), 4).alias("quantity_share"),
        F.percent_rank().over(w7).alias("unit_price_rank_cume_dist_percent_rank"),
        F.corr("unit_price", "quantity").over(w6).alias("corr_unit_price_quantity"),
        F.sum("unit_price_percent_rank").over(w6).alias("product_id_sum_unit_price_percent_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("unit_price") >= 340, "PREMIUM").otherwise("STANDARD").alias("unit_price_tier"),
        "product_id",
    )
    .agg(
        F.percentile_approx("unit_price_rank_cume_dist_percent_rank", [0.25, 0.5, 0.75]).alias("unit_price_rank_cume_dist_percent_rank_quartiles"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.avg("unit_price").alias("avg_unit_price"),
        F.percentile_approx("quantity_share", 0.9).alias("quantity_share_p90"),
        F.sum("unit_price_share").alias("total_unit_price_share"),
        F.count("*").alias("num_rows"),
    )
)
