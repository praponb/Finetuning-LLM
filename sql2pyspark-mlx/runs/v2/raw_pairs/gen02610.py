from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())

result = (
    df_departments
    .filter(F.col("budget") < 1742000)
    .select("id", "dept_name", "budget", F.rank().over(w).alias("rnk"))
    .orderBy(F.col("id").desc())
)
