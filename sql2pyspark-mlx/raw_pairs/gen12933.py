from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("least_cost_price", "id")
w_ordered = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc(), "id")
w3 = Window.partitionBy("category", "brand")
w_grp = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("supplier_id")
w6 = Window.partitionBy(F.year("launch_date"), "supplier_id").orderBy("supplier_id_sum_least_cost_price", "id")
w_recent = Window.partitionBy("brand", "supplier_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("brand").isNotNull())
    .withColumn("least_cost_price", F.least("cost", "price"))
)

enriched = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.ntile(10).over(w_ordered).alias("stock_qty_bucket"),
        F.min("price").over(w3).alias("category_min_price"),
        F.sum("cost").over(w_grp).alias("cumulative_cost"),
        F.sum("least_cost_price").over(w5).alias("supplier_id_sum_least_cost_price"),
    )
)

metrics = (
    enriched
    .filter(F.col("stock_qty_bucket") != 6)
    .select(
        "id",
        "brand",
        "stock_qty",
        "tags",
        "least_cost_price",
        "stock_qty_bucket",
        F.lead("price", 2).over(w6).alias("next_price"),
        F.sum("stock_qty_bucket").over(w_recent).alias("cumulative_stock_qty_bucket"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.order_id").isin([2, 3, 24, 35])), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "brand",
        "stock_qty",
        "tags",
        "stock_qty_bucket",
        "next_price",
        "cumulative_stock_qty_bucket",
    )
)
