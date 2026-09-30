from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy("sale_date", "id")

result = (
    df_store_sales
    .filter(F.col("revenue") > 3500)
    .select(
        "id",
        "product_id",
        "revenue",
        "sale_date",
        F.sum("revenue").over(w).alias("product_id_sum_revenue"),
        F.lag("revenue", 1, 0).over(w2).alias("prev_revenue"),
    )
)
