from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id")
w2 = Window.partitionBy("category", "brand").orderBy("cost", "id")
w3 = Window.partitionBy("brand").orderBy("stock_qty_minus_price", "id")
w4 = Window.partitionBy(F.year("launch_date"), "category")
w5 = Window.partitionBy(F.month("launch_date"), "category").orderBy("stock_qty", "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w7 = Window.orderBy("inventory_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.date_trunc("week", "inventory_last_restock")).orderBy("inventory_quantity", "id")

filtered = (
    df_products
    .filter(F.col("launch_date") <= "2024-10-01")
    .select(
        "*",
        (F.col("stock_qty") - F.col("price")).alias("stock_qty_minus_price"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_filled"),
    )
)

deduped = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    deduped
    .filter(F.col("category") != "Books")
    .select(
        "id",
        "product_name",
        "stock_qty",
        "tags",
        "brand_filled",
        F.ntile(3).over(w2).alias("cost_bucket"),
        F.lead("stock_qty", 1).over(w3).alias("next_stock_qty"),
        F.corr("price", "cost").over(w4).alias("corr_price_cost"),
        F.ntile(3).over(w5).alias("stock_qty_bucket"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.stock_qty",
        "s.tags",
        "s.corr_price_cost",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

kept = (
    with_last
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.store_id").isin([2, 3, 4])), "left_anti")
)

with_windows = (
    kept
    .select(
        "id",
        "inventory_last_restock",
        "inventory_quantity",
        F.max("corr_price_cost").over(w7).alias("running_max_corr_price_cost"),
        F.sum("corr_price_cost").over(w7).alias("running_sum_corr_price_cost"),
        F.avg("corr_price_cost").over(w7).alias("running_avg_corr_price_cost"),
        F.lag("corr_price_cost", 2).over(w8).alias("prev_corr_price_cost"),
    )
)

kept_products = (
    with_windows
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") >= F.col("s.inventory_last_restock")), "left_anti")
)

result = (
    kept_products
    .select(
        "id",
        "inventory_last_restock",
        "running_max_corr_price_cost",
        "running_sum_corr_price_cost",
        "running_avg_corr_price_cost",
        "prev_corr_price_cost",
    )
    .orderBy("running_avg_corr_price_cost", "id")
)
