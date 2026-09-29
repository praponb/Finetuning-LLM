from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("launch_date").desc())

result = (
    df_products
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 5)
    .select("id", "brand", "launch_date")
    .orderBy("launch_date", "brand")
)
