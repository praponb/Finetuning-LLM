from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "revenue",
        "product_id",
        "sale_date",
        F.row_number().over(w).alias("row_num"),
        F.round(F.avg("revenue").over(w2), 2).alias("revenue_moving_avg_7"),
    )
)
