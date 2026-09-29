from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())
w2 = Window.partitionBy("product_id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "revenue",
        F.rank().over(w).alias("rnk"),
        F.avg("revenue").over(w2).alias("product_id_avg_revenue"),
    )
)
