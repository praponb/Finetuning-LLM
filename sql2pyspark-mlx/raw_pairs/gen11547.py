from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("launch_date"), "brand").orderBy("stock_qty")

enriched = (
    df_products
    .select(
        "*",
        F.avg("cost").over(w).alias("running_avg_cost"),
        F.min("cost").over(w).alias("running_min_cost"),
        F.count("cost").over(w).alias("running_count_cost"),
        F.rank().over(w2).alias("stock_qty_rank"),
    )
)

result = (
    enriched
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
    )
)
