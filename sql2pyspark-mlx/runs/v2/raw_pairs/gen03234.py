from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_store_sales
    .filter(F.col("revenue").between(2400, 2800))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.round(F.avg("revenue").over(w), 2).alias("revenue_moving_avg_4"),
    )
    .orderBy(F.col("revenue_moving_avg_4").desc())
)
