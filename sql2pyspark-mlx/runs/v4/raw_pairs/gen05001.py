from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget", "id")

result = (
    df_departments
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") <= 2)
)
