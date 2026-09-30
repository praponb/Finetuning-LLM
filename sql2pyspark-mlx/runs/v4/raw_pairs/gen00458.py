from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue").asc_nulls_last())

cleaned = (
    df_store_sales
    .filter(F.col("revenue").between(1100, 2800))
    .select("*", F.round("revenue", 0).alias("revenue_rounded"))
)

result = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)
