from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("launch_date"), "brand")
w2 = Window.partitionBy("brand", "category").orderBy("cost", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("category", "supplier_id").orderBy("stock_qty", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_products
    .filter(F.col("category") == "Electronics")
    .select(
        "id",
        "product_name",
        "brand",
        "tags",
        "supplier_id",
        F.count("stock_qty").over(w).alias("launch_date_month_count_stock_qty"),
        F.avg("price").over(w2).alias("price_avg_5_prior"),
        F.avg("cost").over(w3).alias("cost_avg_6"),
    )
)
