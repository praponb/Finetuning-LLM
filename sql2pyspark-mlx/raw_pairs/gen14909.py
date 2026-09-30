from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "brand").orderBy("price", "id").rowsBetween(-6, Window.currentRow)
w_running = Window.partitionBy("brand").orderBy("launch_date", "id")

metrics = (
    df_products
    .filter(F.col("category") == "Sports")
    .select(
        "*",
        F.avg("cost").over(w).alias("cost_avg_6"),
        F.lag("cost", 2, 0).over(w_running).alias("prev_cost"),
    )
)

result = (
    metrics
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "cost_avg_6",
    )
    .orderBy(F.col("category").desc_nulls_last(), "stock_qty")
)
