from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget")

result = (
    df_departments
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 5)
    .orderBy(F.col("id").desc())
)
