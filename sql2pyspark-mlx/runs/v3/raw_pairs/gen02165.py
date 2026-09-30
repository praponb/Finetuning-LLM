from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3]))
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 5)
    .select("id", "store_id", "sale_date", "product_id")
    .orderBy(F.col("id").desc(), F.col("store_id").desc())
)
