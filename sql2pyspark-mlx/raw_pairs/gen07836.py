from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy("cost", "id")
w3 = Window.orderBy(F.col("stock_qty").desc(), "id")
w4 = Window.partitionBy("category").orderBy(F.col("stock_qty").desc_nulls_last())
w5 = Window.partitionBy("category").orderBy("stock_qty", "id")
w6 = Window.partitionBy("category").orderBy(F.col("stock_qty_cume_dist").desc(), "id")
w7 = Window.orderBy(F.col("prev_stock_qty").desc_nulls_last())
w8 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("prev_stock_qty", "id")

with_latest = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.cost",
        "s.stock_qty",
        "s.tags",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity"),
    )
)

scored = (
    with_latest
    .select(
        "*",
        F.row_number().over(w2).alias("event_seq"),
        F.ntile(5).over(w3).alias("stock_qty_bucket"),
        F.cume_dist().over(w4).alias("stock_qty_cume_dist"),
    )
)

cleaned = (
    scored
    .filter(F.col("order_item_unit_price").between(260, 290))
    .select(
        "id",
        "category",
        "cost",
        "stock_qty",
        "tags",
        "quantity",
        "stock_qty_bucket",
        "stock_qty_cume_dist",
        F.when(F.col("quantity") >= 8, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low").alias("quantity_level"),
        (F.col("stock_qty_cume_dist") * F.col("event_seq")).alias("stock_qty_cume_dist_times_event_seq"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("stock_qty_cume_dist_times_event_seq") >= 61)
    .select(
        "id",
        "tags",
        "quantity",
        "quantity_level",
        F.lag("stock_qty", 2, 0).over(w5).alias("prev_stock_qty"),
        F.row_number().over(w6).alias("seq_no"),
        F.ntile(10).over(w5).alias("stock_qty_bucket_2"),
    )
)

metrics = (
    with_windows
    .withColumn("prev_stock_qty_dense_rank", F.dense_rank().over(w7))
    .withColumn("first_known_stock_qty_bucket_2", F.first("stock_qty_bucket_2", ignorenulls=True).over(w8))
    .withColumn("next_quantity", F.lead("quantity", 1).over(w9))
)

latest_match = (
    metrics
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.tags",
        "s.quantity",
        "s.quantity_level",
        "s.prev_stock_qty_dense_rank",
        "s.first_known_stock_qty_bucket_2",
        "s.next_quantity",
        F.col("c.quantity").alias("order_item_quantity"),
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("order_item_unit_price") >= 340, "LARGE").otherwise("SMALL").alias("order_item_unit_price_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("next_quantity", [0.25, 0.5, 0.75]).alias("next_quantity_quartiles"),
        F.avg("first_known_stock_qty_bucket_2").alias("avg_first_known_stock_qty_bucket_2"),
    )
    .filter(F.col("avg_first_known_stock_qty_bucket_2") >= 0)
    .orderBy(F.col("cnt").desc_nulls_last())
)
