from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy("launch_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("supplier_id", "category").orderBy("cost", "id")

result = (
    df_products
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.sum("stock_qty").over(w).alias("last3_sum_stock_qty"),
        F.count("stock_qty").over(w).alias("last3_count_stock_qty"),
        F.avg("stock_qty").over(w).alias("last3_avg_stock_qty"),
        F.min("stock_qty").over(w).alias("last3_min_stock_qty"),
        F.row_number().over(w2).alias("row_seq"),
    )
)
