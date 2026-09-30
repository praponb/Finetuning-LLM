from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("WEEK", "sale_date")).orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("units").desc(), "id")
w_part = Window.partitionBy(F.date_trunc("month", "sale_date"), "product_id").orderBy(F.col("revenue").desc(), "id")

result = (
    df_store_sales
    .withColumn("running_revenue", F.sum("revenue").over(w))
    .withColumn("units_bucket", F.ntile(4).over(w2))
    .withColumn("revenue_bucket", F.ntile(3).over(w_part))
)
