from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("units").asc_nulls_last())
w_running = Window.partitionBy(F.month("sale_date"), "product_id").orderBy("revenue", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_store_sales
    .select(
        "*",
        F.cume_dist().over(w).alias("units_cume_dist"),
        F.avg("revenue").over(w_running).alias("revenue_avg_6"),
    )
)
