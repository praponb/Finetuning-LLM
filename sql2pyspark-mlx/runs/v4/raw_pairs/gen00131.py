from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units")

result = (
    df_store_sales
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") == 1)
)
