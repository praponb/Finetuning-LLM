from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating")

result = (
    df_suppliers
    .filter(F.col("country") == "TH")
    .withColumn("row_num", F.dense_rank().over(w))
    .filter(F.col("row_num") == 1)
)
