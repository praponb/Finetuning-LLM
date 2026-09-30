from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())

result = (
    df_departments
    .select("id", "dept_name", "budget", F.dense_rank().over(w).alias("dense_rnk"))
)
