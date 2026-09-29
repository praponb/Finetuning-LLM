from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget")

result = (
    df_departments
    .select("id", "dept_name", "budget", F.dense_rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 1)
    .select("id", "dept_name", "budget")
    .orderBy(F.col("dept_name").desc())
)
