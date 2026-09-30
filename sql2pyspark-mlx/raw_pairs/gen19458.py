from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")
w4 = Window.partitionBy(F.year("sale_date"), "store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("revenue", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_store_sales
    .withColumn("running_revenue", F.sum("revenue").over(w))
    .withColumn("cumulative_revenue", F.sum("revenue").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("last_known_units", F.last("units", ignorenulls=True).over(w4))
    .withColumn("units_avg_2", F.avg("units").over(w5))
)
