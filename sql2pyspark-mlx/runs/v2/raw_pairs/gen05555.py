from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("revenue").between(3100, 3600))
    .select(
        "id",
        "revenue",
        "product_id",
        F.row_number().over(w).alias("revenue_row"),
        F.ntile(3).over(w2).alias("revenue_ntile"),
    )
)
