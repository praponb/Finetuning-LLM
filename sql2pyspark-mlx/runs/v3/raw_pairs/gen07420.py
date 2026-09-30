from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "revenue",
        "sale_date",
        F.min("revenue").over(w).alias("product_id_min_revenue"),
        F.lag("revenue", 2).over(w2).alias("prev_revenue"),
    )
)
