from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("category", "supplier_id").orderBy("launch_date", "id")
w2 = Window.partitionBy("supplier_id").orderBy("price", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("brand", "supplier_id").orderBy("stock_qty", "id")

cleaned = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.round((F.col("cost") * 1.1), 2).alias("cost_scaled"),
    )
)

result = (
    cleaned
    .filter(F.col("brand").isNull())
    .select(
        "id",
        "brand",
        "price",
        "stock_qty",
        "tags",
        F.lag("cost_scaled", 2).over(w_recent).alias("prev_cost_scaled"),
        F.avg("cost_scaled").over(w2).alias("cost_scaled_avg_5"),
        F.ntile(3).over(w3).alias("stock_qty_bucket"),
    )
)
