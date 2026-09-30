from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("stock_qty")
w2 = Window.partitionBy("brand")
w_hist = Window.partitionBy("supplier_id")
w4 = Window.partitionBy("category", "brand").orderBy(F.col("adjusted_stock_qty").desc_nulls_first())
w5 = Window.orderBy(F.col("cost").desc())
w_part = Window.partitionBy("supplier_id", "brand").orderBy(F.col("stock_qty").desc_nulls_last())

kept = (
    df_products
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isin([18, 23, 24, 28])), "left_semi")
)

deduped = (
    kept
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

remaining_keys = (
    deduped
    .select("category", "brand")
    .intersect(
        df_products
        .filter(F.col("cost") <= 320)
        .select("category", "brand")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.category") == F.col("k.category")) & (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.array_contains("tags", "eco"))
    .select(
        "*",
        F.concat_ws("-", "brand", "category").alias("brand_category_key"),
        F.round((F.col("stock_qty") * 1.1), 2).alias("adjusted_stock_qty"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_clean"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") <= F.col("s.launch_date")), "left_anti")
)

cleaned_products = (
    eligible
    .filter(F.col("brand").isNotNull())
    .withColumn("price_minus_stock_qty", (F.col("price") - F.col("stock_qty")))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("Unknown")))
)

metrics = (
    cleaned_products
    .select(
        "*",
        F.corr("stock_qty", "price").over(w2).alias("corr_stock_qty_price"),
        F.stddev_samp("price_minus_stock_qty").over(w_hist).alias("supplier_id_stddev_samp_price_minus_stock_qty"),
        F.cume_dist().over(w4).alias("adjusted_stock_qty_cume_dist"),
        F.percent_rank().over(w5).alias("cost_percent_rank"),
        F.percent_rank().over(w_part).alias("stock_qty_percent_rank"),
    )
)

grouped = (
    metrics
    .groupBy(
        F.when(F.col("corr_stock_qty_price") >= 0.49, "PREMIUM").otherwise("STANDARD").alias("corr_stock_qty_price_tier"),
        F.when(F.col("cost") >= 730, "LARGE").otherwise("SMALL").alias("cost_tier"),
    )
    .agg(
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.max("corr_stock_qty_price").alias("max_corr_stock_qty_price"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    grouped
    .select(
        "corr_stock_qty_price_tier",
        "cost_tier",
        "max_corr_stock_qty_price",
        "num_rows",
    )
)
