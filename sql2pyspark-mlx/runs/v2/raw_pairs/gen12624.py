from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3]))
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        F.round(F.avg("revenue").over(w), 2).alias("revenue_moving_avg_3"),
    )
)
