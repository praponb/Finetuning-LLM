from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .withColumn("corr_revenue_units", F.corr("revenue", "units").over(w))
    .withColumn("revenue_avg_5", F.avg("revenue").over(w2))
    .withColumn("cumulative_units", F.sum("units").over(w3))
    .withColumn("running_units", F.sum("units").over(w_hist))
)
