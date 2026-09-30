from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 5, 6]))
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "store_id", "revenue", "units")
    .orderBy(F.col("id").desc(), "revenue")
)
