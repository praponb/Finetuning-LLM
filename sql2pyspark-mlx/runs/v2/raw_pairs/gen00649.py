from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date")

result = (
    df_store_sales
    .filter(F.col("revenue") > 1800)
    .withColumn("pos", F.rank().over(w))
    .filter(F.col("pos") <= 2)
    .select("id", "product_id", "sale_date")
)
