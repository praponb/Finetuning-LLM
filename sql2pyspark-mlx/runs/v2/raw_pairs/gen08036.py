from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id")

result = (
    df_store_sales
    .filter(F.col("product_id") == 1)
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        F.count("*").over(w).alias("store_id_count"),
        F.round((F.col("revenue") / F.sum("revenue").over(w2)), 3).alias("revenue_share"),
    )
)
