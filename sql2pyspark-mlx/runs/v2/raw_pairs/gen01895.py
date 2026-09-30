from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_store_sales
    .withColumn("min_revenue_by_product", F.min("revenue").over(w))
    .filter(F.col("revenue") == F.col("min_revenue_by_product"))
    .select("id", "product_id", "revenue", "store_id")
    .orderBy(F.col("revenue").desc())
)
