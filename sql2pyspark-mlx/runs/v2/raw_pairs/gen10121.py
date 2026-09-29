from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")

result = (
    df_store_sales
    .withColumn("avg_revenue_by_store", F.avg("revenue").over(w))
    .filter(F.col("revenue") > F.col("avg_revenue_by_store"))
    .select("id", "store_id", "revenue", "product_id")
)
