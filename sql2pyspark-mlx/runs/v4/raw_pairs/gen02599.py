from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("cost").asc_nulls_first())
w_ordered = Window.partitionBy("brand", "supplier_id").orderBy("launch_date", "id")
w_hist = Window.orderBy("launch_date", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_products
    .withColumn("cost_percent_rank", F.percent_rank().over(w))
    .withColumn("prev_stock_qty", F.lag("stock_qty", 1).over(w_ordered))
    .withColumn("price_avg_5", F.avg("price").over(w_hist))
    .withColumn("last_known_brand", F.last("brand", ignorenulls=True).over(w4))
    .withColumn("stock_qty_avg_3", F.avg("stock_qty").over(w5))
)
