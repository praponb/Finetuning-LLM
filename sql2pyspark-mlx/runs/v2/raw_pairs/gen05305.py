from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .withColumn("pos", F.row_number().over(w))
    .filter(F.col("pos") <= 3)
)
