from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue")

result = (
    df_store_sales
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "store_id", "revenue")
    .orderBy(F.col("id").desc(), F.col("store_id").desc())
)
