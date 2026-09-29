from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "category")
w2 = Window.partitionBy("category").orderBy("cost", "id")
w3 = Window.partitionBy("supplier_id")
w4 = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.col("stock_qty") > 270))
    .withColumn("sorted_tags", F.sort_array("tags"))
)

result = (
    valid
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "launch_date",
        "tags",
        "supplier_id",
        "sorted_tags",
        F.corr("cost", "stock_qty").over(w).alias("corr_cost_stock_qty"),
        F.lag("stock_qty", 1).over(w2).alias("prev_stock_qty"),
        F.corr("price", "cost").over(w3).alias("corr_price_cost"),
        F.avg("price").over(w4).alias("price_avg_2_prior"),
        F.sum("stock_qty").over(w5).alias("running_stock_qty"),
    )
)
