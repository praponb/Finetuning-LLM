from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost", "id")
w2 = Window.orderBy(F.col("stock_qty").asc_nulls_first())
w3 = Window.partitionBy("brand", "category").orderBy("stock_qty", "id")

cleaned = (
    df_products
    .filter(F.col("brand").isNotNull() & (~F.col("brand").eqNullSafe(F.lit("Initech"))))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "tags",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "stock_qty",
        "tags",
        F.ntile(4).over(w).alias("cost_bucket"),
        F.rank().over(w2).alias("stock_qty_rank"),
        F.lag("cost", 2, 0).over(w3).alias("prev_cost"),
    )
)
