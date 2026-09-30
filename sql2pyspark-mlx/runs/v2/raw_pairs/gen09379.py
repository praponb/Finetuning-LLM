from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")

result = (
    df_store_sales
    .withColumn("max_revenue_by_product", F.max("revenue").over(w))
    .filter(F.col("revenue") == F.col("max_revenue_by_product"))
    .select("id", "product_id", "revenue")
    .orderBy(F.col("product_id").desc())
)
