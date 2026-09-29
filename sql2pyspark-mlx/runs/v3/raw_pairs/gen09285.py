from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget")

result = (
    df_departments
    .withColumn("rn", F.dense_rank().over(w))
    .filter(F.col("rn") <= 2)
    .select("id", "dept_name", "budget", "location")
)
