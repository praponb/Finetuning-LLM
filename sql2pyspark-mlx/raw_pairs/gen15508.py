from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc())

result = (
    df_store_sales
    .filter(F.col("revenue") < 2200)
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 2)
)
