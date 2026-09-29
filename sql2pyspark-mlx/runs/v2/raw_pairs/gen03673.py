from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_revenue").desc())

result = (
    df_store_sales
    .filter(F.year("sale_date") == 2025)
    .groupBy("store_id")
    .agg(F.sum("revenue").alias("total_revenue"))
    .withColumn("revenue_rank", F.rank().over(w))
    .orderBy(F.col("store_id").desc())
)
