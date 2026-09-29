from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.orderBy(F.col("launch_date").desc(), "id")
w3 = Window.orderBy("order_item_unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("quantity_tier").orderBy(F.col("quantity_p95").desc())

valid = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-06-01"))) & (F.lower(F.trim("product_name")) == "office chair"))
    .select(
        "id",
        "product_name",
        "category",
        "stock_qty",
        "launch_date",
        "tags",
        F.ceil("stock_qty").alias("stock_qty_whole"),
        F.floor("price").alias("price_whole"),
    )
)

filtered = (
    valid
    .filter(F.col("category") == "Grocery")
    .select(
        "id",
        "stock_qty",
        "launch_date",
        "tags",
        "stock_qty_whole",
        F.array_join("tags", ", ").alias("tags_text"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

with_windows = (
    with_last
    .filter(F.col("stock_qty_whole") == 34)
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("last_known_stock_qty", F.last("stock_qty", ignorenulls=True).over(w3))
)

with_last_products = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.launch_date",
        "s.tags",
        "s.tags_text",
        "s.event_seq",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

grouped = (
    with_last_products
    .groupBy(
        F.when(F.col("quantity") >= 253, "HIGH").otherwise("LOW").alias("quantity_tier"),
        F.date_trunc("MONTH", "inventory_last_restock").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.max("event_seq").alias("max_event_seq"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("quantity_p95") < 1000000)
    .withColumn("quantity_p95_percent_rank", F.percent_rank().over(w5))
)

result = (
    grouped
    .select(
        "quantity_tier",
        "activity_month",
        "event_seq_quartiles",
        "max_event_seq",
        "quantity_p95",
        "distinct_ids",
    )
)
