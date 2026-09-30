from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())

result = (
    df_departments
    .filter(F.col("dept_name") == "Marketing")
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") <= 2)
)
