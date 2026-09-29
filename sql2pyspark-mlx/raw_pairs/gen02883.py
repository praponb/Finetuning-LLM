from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w_part = Window.partitionBy(F.date_trunc("week", "sale_date"), "product_id").orderBy("sale_date", "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("units").asc_nulls_first())
w4 = Window.orderBy("sale_date", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("prev_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.month("launch_date"), "product_id").orderBy("prev_revenue", "id")
w8 = Window.partitionBy("product_id").orderBy("price", "id")
w_hist = Window.orderBy(F.col("price").asc_nulls_first())
w_running = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running_2 = Window.partitionBy("category").orderBy(F.col("stock_qty").asc_nulls_first())
w12 = Window.orderBy("last_known_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy("product_id").orderBy("running_last_known_price", "id").rowsBetween(-5, -1)
w14 = Window.partitionBy("category")

metrics = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        F.round((F.col("units") / F.sum("units").over(w)), 4).alias("units_share"),
        F.lag("revenue", 2).over(w_part).alias("prev_revenue"),
        F.rank().over(w3).alias("units_rank"),
        F.avg("revenue").over(w4).alias("revenue_avg_2"),
    )
)

enriched = (
    metrics
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .filter(F.col("p.product_name").like("Lap%"))
    .select(
        "e.id",
        "e.product_id",
        "e.prev_revenue",
        F.col("p.launch_date"),
        F.col("p.price"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "product_id",
        "prev_revenue",
        "launch_date",
        "price",
        F.sum("prev_revenue").over(w5).alias("cumulative_prev_revenue"),
        F.sum("prev_revenue").over(w6).alias("running_prev_revenue"),
        F.lead("price", 2).over(w7).alias("next_price"),
        F.last("price", ignorenulls=True).over(w6).alias("last_known_price"),
        F.ntile(4).over(w8).alias("price_bucket"),
    )
)

joined = (
    with_windows
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select(
        "e.id",
        "e.product_id",
        "e.price",
        "e.cumulative_prev_revenue",
        "e.last_known_price",
        "e.price_bucket",
        F.col("p.category"),
        F.col("p.stock_qty"),
        F.col("p.launch_date").alias("product_launch_date"),
    )
)

metrics_store_sales = (
    joined
    .select(
        "*",
        F.dense_rank().over(w_hist).alias("price_dense_rank"),
        F.sum("last_known_price").over(w_running).alias("running_last_known_price"),
        F.cume_dist().over(w_running_2).alias("stock_qty_cume_dist"),
    )
)

result = (
    metrics_store_sales
    .withColumn("running_stock_qty_cume_dist", F.sum("stock_qty_cume_dist").over(w12))
    .withColumn("stock_qty_avg_5_prior", F.avg("stock_qty").over(w13))
    .withColumn("running_last_known_price_share", F.round((F.col("running_last_known_price") / F.sum("running_last_known_price").over(w14)), 4))
)
