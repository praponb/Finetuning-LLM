from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id")

result = (
    df_stores
    .filter(F.col("city") == "Singapore")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") <= 1)
    .orderBy(F.col("store_name").desc(), "city")
)
