from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("units", "id")

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 5, 6, 7]))
    .select(
        "id",
        "sale_date",
        "revenue",
        "product_id",
        "store_id",
        "units",
        F.lag("revenue", 1).over(w).alias("prev_revenue"),
        F.first("revenue").over(w2).alias("first_revenue"),
    )
)
