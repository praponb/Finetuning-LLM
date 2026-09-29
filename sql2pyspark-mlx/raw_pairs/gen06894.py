from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty_minus_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy("stock_qty_minus_cost", "id")

filtered = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-03-31").cast("date")) & (~F.col("brand").eqNullSafe(F.lit("Acme"))))
    .select(
        "*",
        (F.col("stock_qty") - F.col("cost")).alias("stock_qty_minus_cost"),
        F.concat_ws("-", "brand", "product_name").alias("brand_product_name_key"),
    )
)

result = (
    filtered
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        F.count("cost").over(w).alias("running_count_cost"),
        F.avg("cost").over(w).alias("running_avg_cost"),
        F.min("cost").over(w).alias("running_min_cost"),
        F.sum("cost").over(w).alias("running_sum_cost"),
        F.ntile(10).over(w2).alias("stock_qty_minus_cost_bucket"),
    )
)
