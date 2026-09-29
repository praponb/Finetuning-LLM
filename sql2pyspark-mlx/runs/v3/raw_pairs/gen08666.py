from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())

result = (
    df_departments
    .filter(F.col("budget") <= 2398000)
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 3)
    .select("id", "dept_name", "budget")
)
