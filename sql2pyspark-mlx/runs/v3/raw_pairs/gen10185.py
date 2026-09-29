from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")

result = (
    df_store_sales
    .filter(F.col("store_id") == 3)
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") == 1)
    .select("id", "store_id", "units", "revenue")
    .orderBy(F.col("store_id").desc(), F.col("revenue").desc())
)
