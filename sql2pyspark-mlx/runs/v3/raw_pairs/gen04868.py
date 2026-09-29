from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("revenue", "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.round(F.avg("revenue").over(w), 2).alias("revenue_moving_avg_7"),
        F.row_number().over(w2).alias("rn"),
    )
)
