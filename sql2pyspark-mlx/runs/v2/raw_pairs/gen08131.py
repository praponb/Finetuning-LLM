from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 8]))
    .select(
        "id",
        "product_id",
        "revenue",
        "sale_date",
        F.row_number().over(w).alias("row_num"),
        F.round(F.avg("revenue").over(w2), 2).alias("revenue_moving_avg_4"),
    )
)
