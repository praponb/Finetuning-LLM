from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("days_since_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w3 = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id")
w4 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("last_restock_minus_27d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("brand")
w7 = Window.partitionBy(F.month("launch_date"), "brand").orderBy(F.col("cumulative_days_since_last_restock").asc_nulls_last())
w_recent = Window.orderBy(F.col("stock_qty").asc_nulls_first())

cleaned = (
    df_inventory
    .filter(F.col("last_restock").between("2023-04-01", "2023-09-15"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
        F.date_sub("last_restock", 27).alias("last_restock_minus_27d"),
    )
)

enriched = (
    cleaned
    .filter(F.col("quantity").isNotNull())
    .select(
        "id",
        "product_id",
        "days_since_last_restock",
        F.sum("days_since_last_restock").over(w).alias("cumulative_days_since_last_restock"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(10).over(w3).alias("quantity_bucket"),
        F.first("quantity", ignorenulls=True).over(w4).alias("first_known_quantity"),
        F.last("quantity", ignorenulls=True).over(w5).alias("last_known_quantity"),
    )
)

remaining_keys = (
    enriched
    .select("id")
    .subtract(
        df_inventory
        .filter(F.col("product_id") == 8)
        .select("id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.stock_qty"), F.col("b.launch_date"), F.col("b.brand"))
)

with_windows = (
    joined
    .select(
        "*",
        F.min("stock_qty").over(w6).alias("brand_min_stock_qty"),
        F.rank().over(w7).alias("cumulative_days_since_last_restock_rank"),
        F.percent_rank().over(w_recent).alias("stock_qty_percent_rank"),
    )
)

result = (
    with_windows
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .rollup(
        F.col("d.brand").alias("product_brand"),
        F.col("a.brand").alias("inventory_brand"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("a.quantity_bucket")).alias("max_quantity_bucket"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("product_brand").asc_nulls_last())
)
