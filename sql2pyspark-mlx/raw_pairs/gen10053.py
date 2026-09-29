from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "order_id").orderBy(F.col("launch_date").desc())
w2 = Window.partitionBy("category", "product_id").orderBy("dedup_rank", "id")
w3 = Window.partitionBy("order_id", "product_id").orderBy("launch_date", "id")
w4 = Window.orderBy("unit_price", "id")
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.min("quantity").alias("min_order_item_quantity"))

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", F.col("unit_price").cast("int").alias("unit_price_int"))
)

with_dims = (
    base
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.quantity",
        "src.unit_price",
        "src.unit_price_int",
        F.col("d.category"),
        F.col("d.launch_date"),
        F.col("d.product_name"),
    )
)

ranked = (
    with_dims
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_windows = (
    ranked
    .select(
        "*",
        F.lag("unit_price_int", 1).over(w2).alias("prev_unit_price_int"),
        F.row_number().over(w3).alias("event_seq"),
        F.ntile(4).over(w4).alias("unit_price_bucket"),
    )
)

kept_keys = (
    with_windows
    .select("product_id")
    .intersect(
        df_order_items
        .filter(F.col("quantity").between(5, 8))
        .select("product_id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

wide = (
    verified
    .groupBy("order_id", "product_id")
    .pivot("category", ["Electronics", "Books", "Fashion"])
    .agg(F.avg("unit_price").alias("total"))
)

result = (
    wide
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "order_id",
        "product_id",
        "Electronics",
        "Books",
        "Fashion",
        F.col("min_order_item_quantity"),
    )
    .orderBy("Fashion", F.col("min_order_item_quantity").desc())
)
