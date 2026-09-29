from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w_running = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-08-17").cast("date"))
    .select(
        "id",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        (F.col("price") - F.col("stock_qty")).alias("price_minus_stock_qty"),
        F.date_trunc("WEEK", "launch_date").alias("launch_date_period"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_filled"),
    )
)

latest_match = (
    valid
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.unit_price").alias("order_item_unit_price"),
        F.col("c.quantity"),
    )
)

with_windows = (
    latest_match
    .filter(F.col("price") >= 410)
    .select(
        "id",
        "price",
        "stock_qty",
        "price_minus_stock_qty",
        "launch_date_period",
        "brand_filled",
        F.min("quantity").over(w_running).alias("running_min_quantity"),
        F.avg("quantity").over(w_running).alias("running_avg_quantity"),
        F.sum("quantity").over(w_running).alias("running_sum_quantity"),
        F.count("quantity").over(w_running).alias("running_count_quantity"),
    )
)

matched_keys = (
    with_windows
    .select("id")
    .intersect(
        df_products
        .filter(F.col("brand") == "Globex")
        .select("id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.quantity") >= 4), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "price",
        "stock_qty",
        "launch_date_period",
        "brand_filled",
        "running_min_quantity",
        "running_avg_quantity",
        "running_sum_quantity",
        "running_count_quantity",
    )
    .orderBy(F.col("launch_date_period").desc_nulls_last())
)
