from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "brand").orderBy("price", "id").rowsBetween(-6, -1)
w2 = Window.orderBy("stock_qty", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy(F.year("launch_date")).orderBy(F.col("price").asc_nulls_last())
w_ordered = Window.partitionBy(F.year("launch_date"), "category").orderBy("launch_date", "id").rowsBetween(-6, Window.currentRow)
w_ordered_2 = Window.orderBy(F.col("cost").desc(), "id")

result = (
    df_products
    .select(
        "id",
        "product_name",
        "cost",
        "stock_qty",
        "launch_date",
        F.avg("stock_qty").over(w).alias("stock_qty_avg_6_prior"),
        F.avg("price").over(w2).alias("price_avg_3_prior"),
        F.percent_rank().over(w3).alias("price_percent_rank"),
        F.avg("cost").over(w_ordered).alias("cost_avg_6"),
        F.ntile(4).over(w_ordered_2).alias("cost_bucket"),
    )
)
