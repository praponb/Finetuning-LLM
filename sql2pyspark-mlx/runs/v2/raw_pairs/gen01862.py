from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy("budget")

result = (
    df_departments
    .withColumn("pos", F.dense_rank().over(w))
    .filter(F.col("pos") <= 2)
    .select("id", "location", "budget", "dept_name")
)
