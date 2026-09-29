from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("unit_price").desc(), "id")
w_running = Window.partitionBy("brand", "product_id").orderBy("quantity_filled", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy(F.col("quantity_filled").desc(), "id")
w_grp = Window.partitionBy("product_id").orderBy("seq_no", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("brand").orderBy("seq_no", "id")
w_hist = Window.partitionBy("product_id", "brand").orderBy("last7_count_quantity_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist_2 = Window.partitionBy("product_id").orderBy("unit_price", "id")
w_hist_3 = Window.partitionBy("brand").orderBy("last3_avg_quantity_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("product_id", "brand").orderBy("seq_no", "id")
w10 = Window.partitionBy("brand").orderBy(F.col("last3_min_quantity_filled").desc(), "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "product_id", "unit_price", "unit_price_filled", "quantity_filled")
)

joined = (
    latest
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.stock_qty") <= 351)
    .select(
        "src.id",
        "src.product_id",
        "src.unit_price",
        "src.quantity_filled",
        F.col("b.brand"),
    )
)

scored = (
    joined
    .select(
        "id",
        "product_id",
        "unit_price",
        "quantity_filled",
        "brand",
        F.avg("quantity_filled").over(w_running).alias("last3_avg_quantity_filled"),
        F.max("quantity_filled").over(w_running).alias("last3_max_quantity_filled"),
        F.min("quantity_filled").over(w_running).alias("last3_min_quantity_filled"),
        F.sum("quantity_filled").over(w_running).alias("last3_sum_quantity_filled"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

scored_order_items = (
    scored
    .withColumn("last7_count_quantity_filled", F.count("quantity_filled").over(w_grp))
    .withColumn("last7_avg_quantity_filled", F.avg("quantity_filled").over(w_grp))
    .withColumn("last7_min_quantity_filled", F.min("quantity_filled").over(w_grp))
    .withColumn("event_seq", F.row_number().over(w5))
)

result = (
    scored_order_items
    .select(
        "id",
        "unit_price",
        "quantity_filled",
        "last3_max_quantity_filled",
        "last7_count_quantity_filled",
        "last7_min_quantity_filled",
        F.sum("last7_min_quantity_filled").over(w_hist).alias("running_last7_min_quantity_filled"),
        F.lag("last7_count_quantity_filled", 2).over(w_hist_2).alias("prev_last7_count_quantity_filled"),
        F.sum("event_seq").over(w_hist_3).alias("running_event_seq"),
        F.ntile(10).over(w9).alias("seq_no_bucket"),
        F.row_number().over(w10).alias("event_seq_2"),
    )
)
