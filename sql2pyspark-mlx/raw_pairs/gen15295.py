from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w_grp = Window.partitionBy("order_id")
w_hist = Window.partitionBy("order_id", "product_id")
w4 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc_nulls_last())
w5 = Window.orderBy(F.col("quantity").desc())
w6 = Window.partitionBy("product_id", "order_id")
w_running = Window.orderBy(F.col("unit_price").desc())
w_running_2 = Window.orderBy(F.col("quantity_share").desc())
w_recent = Window.orderBy(F.col("unit_price_cume_dist").desc())
w10 = Window.orderBy(F.col("quantity_rank").asc_nulls_first())
w11 = Window.orderBy(F.col("unit_price_cume_dist_2_int").asc_nulls_first())
w12 = Window.orderBy("unit_price_cume_dist_2")

matched_keys = (
    df_order_items
    .select("product_id", "id")
    .exceptAll(
        df_order_items
        .filter(F.col("unit_price") <= 340)
        .select("product_id", "id")
    )
)

reconciled = (
    df_order_items
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "*",
        F.count("unit_price").over(w).alias("product_id_count_unit_price"),
        F.corr("unit_price", "quantity").over(w_grp).alias("corr_unit_price_quantity"),
        F.corr("quantity", "unit_price").over(w_hist).alias("corr_quantity_unit_price"),
        F.cume_dist().over(w4).alias("unit_price_cume_dist"),
        F.round((F.col("quantity") / F.sum("quantity").over(w)), 4).alias("quantity_share"),
    )
)

scored_order_items = (
    scored
    .select(
        "unit_price",
        "product_id_count_unit_price",
        "corr_quantity_unit_price",
        "unit_price_cume_dist",
        "quantity_share",
        F.rank().over(w5).alias("quantity_rank"),
        F.max("quantity_share").over(w6).alias("product_id_max_quantity_share"),
    )
)

with_windows = (
    scored_order_items
    .select(
        "product_id_count_unit_price",
        "corr_quantity_unit_price",
        "quantity_rank",
        "product_id_max_quantity_share",
        F.cume_dist().over(w_running).alias("unit_price_cume_dist_2"),
        F.percent_rank().over(w_running_2).alias("quantity_share_percent_rank"),
        F.percent_rank().over(w_recent).alias("unit_price_cume_dist_percent_rank"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("product_id_max_quantity_share") <= 0)
    .withColumn("quantity_rank_per_corr_quantity_unit_price", (F.col("quantity_rank") / F.nullif("corr_quantity_unit_price", F.lit(0))))
    .withColumn("unit_price_cume_dist_2_int", F.col("unit_price_cume_dist_2").cast("int"))
)

metrics = (
    cleaned
    .withColumn("quantity_rank_percent_rank", F.percent_rank().over(w10))
    .withColumn("unit_price_cume_dist_2_int_cume_dist", F.cume_dist().over(w11))
    .withColumn("unit_price_cume_dist_2_percent_rank", F.percent_rank().over(w12))
)

result = (
    metrics
    .groupBy(
        F.when(F.col("unit_price_cume_dist_2_int_cume_dist") >= 0.62, "HIGH").otherwise("LOW").alias("unit_price_cume_dist_2_int_cume_dist_tier"),
        F.when(F.col("quantity_rank_percent_rank") >= 0.62, "LARGE").otherwise("SMALL").alias("quantity_rank_percent_rank_tier"),
        F.when(F.col("product_id_max_quantity_share") >= 1, "HIGH").otherwise("LOW").alias("product_id_max_quantity_share_tier"),
    )
    .agg(
        F.percentile_approx("unit_price_cume_dist_2", 0.5).alias("unit_price_cume_dist_2_p50"),
        F.round(F.avg("quantity_share_percent_rank"), 2).alias("avg_quantity_share_percent_rank"),
        F.count("*").alias("row_count"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("quantity_rank", [0.25, 0.5, 0.75]).alias("quantity_rank_quartiles"),
        F.count("*").alias("num_rows_2"),
    )
)
