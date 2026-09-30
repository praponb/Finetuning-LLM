from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("product_id", "order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id")
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.max("units").alias("max_store_sale_units"))

metrics = (
    df_order_items
    .filter(F.col("quantity").between(4, 6))
    .select(
        "*",
        F.count("quantity").over(w_ordered).alias("running_count_quantity"),
        F.avg("quantity").over(w_ordered).alias("running_avg_quantity"),
        F.sum("quantity").over(w_ordered).alias("running_sum_quantity"),
        F.min("quantity").over(w_ordered).alias("running_min_quantity"),
        F.ntile(3).over(w2).alias("quantity_bucket"),
    )
)

filtered = (
    metrics
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "running_sum_quantity",
        "running_min_quantity",
        "quantity_bucket",
        F.greatest("quantity_bucket", "running_avg_quantity").alias("greatest_quantity_bucket_running_avg_quantity"),
        F.when(F.col("unit_price") >= 420, "High").when(F.col("unit_price") >= 150, "Medium").otherwise("Low").alias("unit_price_band"),
    )
)

kept_keys = (
    filtered
    .select("product_id")
    .intersect(
        df_order_items
        .filter(F.col("order_id").isin([10, 36, 40]))
        .select("product_id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.running_sum_quantity",
        "src.running_min_quantity",
        "src.quantity_bucket",
        "src.greatest_quantity_bucket_running_avg_quantity",
        F.col("p.stock_qty"),
        F.col("p.price"),
    )
)

result = (
    joined
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "running_sum_quantity",
        "running_min_quantity",
        "quantity_bucket",
        "greatest_quantity_bucket_running_avg_quantity",
        "stock_qty",
        "price",
        F.col("max_store_sale_units"),
    )
)
