from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("category")
w2 = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc(), "id")
w3 = Window.orderBy("stock_qty")

enriched = (
    df_products
    .select(
        "*",
        F.corr("cost", "price").over(w_ordered).alias("corr_cost_price"),
        F.ntile(4).over(w2).alias("stock_qty_bucket"),
        F.dense_rank().over(w3).alias("stock_qty_dense_rank"),
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
    .orderBy(F.col("brand").asc_nulls_first(), "id")
)
