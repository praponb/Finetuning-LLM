from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("margin", "id")
w2 = Window.partitionBy("supplier_id")

base = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-05-24").cast("date"))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "supplier_id",
        (F.col("price") * F.col("cost")).alias("margin"),
        (F.col("price") * F.col("cost")).alias("margin_2"),
    )
)

result = (
    base
    .select(
        "*",
        F.lag("stock_qty", 1).over(w).alias("prev_stock_qty"),
        F.sum("cost").over(w2).alias("supplier_id_sum_cost"),
    )
)
