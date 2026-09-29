from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("supplier_id").orderBy("price", "id")
w_part = Window.partitionBy("category").orderBy(F.col("price").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

cleaned = (
    df_products
    .filter(F.col("stock_qty") >= 173)
    .select(
        "id",
        "category",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.dayofweek("launch_date").alias("launch_date_dayofweek"),
        F.array_join("tags", "|").alias("tags_text"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "category",
        "tags",
        "launch_date_dayofweek",
        F.lead("launch_date_dayofweek").over(w_running).alias("next_launch_date_dayofweek"),
        F.lag("launch_date_dayofweek").over(w_running).alias("prev_launch_date_dayofweek"),
        F.lag("stock_qty").over(w_running).alias("prev_stock_qty"),
        F.row_number().over(w_running).alias("seq_no"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.quantity") <= 8), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.next_launch_date_dayofweek",
        "s.prev_stock_qty",
        "s.seq_no",
        "s.event_seq",
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

valid = (
    latest_match
    .filter(F.col("category") == "Home")
    .select(
        "*",
        (F.col("order_item_quantity") - F.col("seq_no")).alias("order_item_quantity_minus_seq_no"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.next_launch_date_dayofweek",
        "s.event_seq",
        F.col("c.quantity"),
    )
)

stats = (
    with_latest
    .groupBy(
        F.when(F.col("quantity") >= 198, "HIGH").otherwise("LOW").alias("quantity_tier"),
    )
    .agg(
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_rows"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_rows_2"),
    )
)

result = (
    stats
    .select("quantity_tier", "sports_rows", "avg_quantity")
    .orderBy(F.col("avg_quantity").desc_nulls_last())
)
