from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("units", "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "product_id",
        "units",
        F.round(F.avg("revenue").over(w), 2).alias("revenue_moving_avg_3"),
        F.row_number().over(w2).alias("row_num"),
    )
    .orderBy(F.col("store_id").desc())
)
