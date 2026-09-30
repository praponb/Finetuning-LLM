from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("brand").orderBy("stock_qty")

result = (
    df_products
    .filter(F.col("supplier_id").isin([20, 23, 24]))
    .select(
        "id",
        "brand",
        "cost",
        "stock_qty",
        F.sum("cost").over(w).alias("brand_sum_cost"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
