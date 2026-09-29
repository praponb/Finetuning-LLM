from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_revenue").desc())

result = (
    df_store_sales
    .filter(F.col("revenue").between(1300, 3400))
    .groupBy("product_id")
    .agg(F.sum("revenue").alias("total_revenue"))
    .withColumn("rnk", F.rank().over(w))
    .orderBy(F.col("rnk").desc(), F.col("total_revenue").desc())
)
