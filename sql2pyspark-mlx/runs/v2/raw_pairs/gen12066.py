from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue")
w2 = Window.partitionBy("store_id", "product_id")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4]))
    .select(
        "id",
        "store_id",
        "revenue",
        "product_id",
        F.dense_rank().over(w).alias("revenue_dense_rank"),
        F.round((F.col("revenue") / F.sum("revenue").over(w2)), 3).alias("revenue_share"),
    )
)
