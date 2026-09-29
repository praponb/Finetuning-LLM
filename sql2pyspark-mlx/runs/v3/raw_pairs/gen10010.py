from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())

result = (
    df_departments
    .filter(F.col("location") != "Bangkok")
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 1)
)
