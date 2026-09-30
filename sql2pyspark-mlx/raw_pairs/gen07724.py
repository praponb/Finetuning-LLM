from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("launch_date")).orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("brand").orderBy("cost", "id")
w3 = Window.partitionBy(F.date_trunc("month", "launch_date"), "category")

result = (
    df_products
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "tags",
        F.ntile(3).over(w).alias("cost_bucket"),
        F.lag("stock_qty", 2, 0).over(w2).alias("prev_stock_qty"),
        F.stddev_samp("stock_qty").over(w3).alias("launch_date_period_stddev_samp_stock_qty"),
    )
)
