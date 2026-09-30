from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget")

result = (
    df_departments
    .select("id", "dept_name", "budget", "location", F.dense_rank().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .orderBy(F.col("location").desc(), F.col("rn").desc())
)
