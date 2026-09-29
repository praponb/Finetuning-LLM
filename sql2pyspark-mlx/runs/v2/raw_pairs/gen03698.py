from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("store_id").orderBy("revenue")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "revenue",
        "store_id",
        F.min("revenue").over(w).alias("product_id_min_revenue"),
        F.rank().over(w2).alias("rnk"),
    )
)
