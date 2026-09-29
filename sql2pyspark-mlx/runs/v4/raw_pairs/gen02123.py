from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())

result = (
    df_departments
    .filter(F.col("budget") <= 1344000)
    .withColumn("rnk", F.dense_rank().over(w))
    .filter(F.col("rnk") <= 5)
    .select("id", "location", "budget", "dept_name")
)
