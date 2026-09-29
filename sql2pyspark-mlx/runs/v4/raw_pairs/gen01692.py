from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("line_total", "id")
w2 = Window.orderBy("quantity", "id")
w3 = Window.partitionBy("product_id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("quantity") <= 5))
    .withColumn("quantity_str", F.col("quantity").cast("double"))
    .withColumn("line_total", (F.col("unit_price") - F.col("quantity")))
)

with_windows = (
    valid
    .select(
        "*",
        F.lead("unit_price", 1, 0).over(w).alias("next_unit_price"),
        F.ntile(4).over(w2).alias("quantity_bucket"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w3)), 4).alias("unit_price_share"),
    )
)

kept_keys = (
    with_windows
    .select("order_id", "id")
    .exceptAll(
        df_order_items
        .filter(F.col("unit_price") > 270)
        .select("order_id", "id")
    )
)

verified = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "line_total",
        "next_unit_price",
        "quantity_bucket",
        F.round((F.col("next_unit_price") / F.col("unit_price")), 2).alias("next_unit_price_per_unit_price"),
    )
)

valid_order_items = (
    base
    .filter(F.col("quantity") < 8)
    .select(
        "product_id",
        "quantity",
        "line_total",
        "next_unit_price",
        "quantity_bucket",
        "next_unit_price_per_unit_price",
        (F.col("line_total") - F.col("next_unit_price")).alias("line_total_minus_next_unit_price"),
    )
)

with_dims = (
    valid_order_items
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.category"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select(
        "e.*",
        F.col("b.product_name"),
        F.col("b.launch_date"),
        F.col("b.stock_qty"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("QUARTER", "launch_date").alias("quarter_start"))
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.count_if(F.col("category") == "Home").alias("home_events"),
        F.round(F.sum("quantity") / F.nullif(F.count(F.when(F.col("category") == "Books", 1)), F.lit(0)), 2).alias("quantity_per_books"),
    )
    .filter(F.col("home_events") > 1)
    .orderBy(F.col("quantity_bit_or").asc_nulls_last())
)
