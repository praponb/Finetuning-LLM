from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty_whole", "id")
w2 = Window.partitionBy("category", "supplier_id").orderBy(F.col("stock_qty_num").asc_nulls_last())
w3 = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-06-04").cast("date"))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.floor("stock_qty").alias("stock_qty_whole"),
        F.col("stock_qty").cast("double").alias("stock_qty_num"),
    )
)

result = (
    base
    .select(
        "id",
        "product_name",
        "brand",
        "launch_date",
        "supplier_id",
        "stock_qty_whole",
        "stock_qty_num",
        F.lag("stock_qty_whole", 1).over(w).alias("prev_stock_qty_whole"),
        F.cume_dist().over(w2).alias("stock_qty_num_cume_dist"),
        F.avg("cost").over(w3).alias("cost_avg_6"),
    )
)
