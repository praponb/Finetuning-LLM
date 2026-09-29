from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget")

result = (
    df_departments
    .select("id", "dept_name", "budget", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 5)
    .orderBy("pos", F.col("dept_name").desc())
)
