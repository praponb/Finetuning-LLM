from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("product_id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "units",
        "product_id",
        "revenue",
        F.row_number().over(w).alias("row_num"),
        F.sum("revenue").over(w2).alias("product_id_sum_revenue"),
    )
)
