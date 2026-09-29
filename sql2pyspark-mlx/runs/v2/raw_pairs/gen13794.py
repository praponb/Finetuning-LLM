from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc())

result = (
    df_store_sales
    .filter(F.col("revenue") < 1300)
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") <= 3)
    .select("id", "store_id", "units", "sale_date")
)
