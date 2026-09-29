from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc())
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "revenue",
        "sale_date",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.lag("revenue", 1).over(w2).alias("prev_revenue"),
    )
)
