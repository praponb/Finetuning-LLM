from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date")
w2 = Window.partitionBy("product_id")

result = (
    df_store_sales
    .filter(F.col("units") == 12)
    .select(
        "id",
        "sale_date",
        "product_id",
        "revenue",
        F.dense_rank().over(w).alias("sale_date_dense_rank"),
        F.round((F.col("revenue") / F.sum("revenue").over(w2)), 3).alias("revenue_share"),
    )
    .orderBy("revenue")
)
