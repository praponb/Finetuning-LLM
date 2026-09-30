from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("city") == "Chiang Mai")
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 5)
    .orderBy("country")
)
