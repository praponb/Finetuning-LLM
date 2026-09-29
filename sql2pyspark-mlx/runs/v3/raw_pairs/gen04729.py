from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sale_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_store_sales
    .select(
        "id",
        "sale_date",
        "revenue",
        F.round(F.avg("revenue").over(w), 2).alias("revenue_moving_avg_3"),
    )
    .orderBy(F.col("revenue_moving_avg_3").desc())
)
