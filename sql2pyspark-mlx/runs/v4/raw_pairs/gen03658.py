from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc())

result = (
    df_store_sales
    .filter(F.col("product_id").isin([5, 7]))
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 2)
    .select("id", "store_id", "sale_date", "revenue")
)
