from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())

result = (
    df_departments
    .select("id", "location", "budget", "dept_name", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "location", "budget", "dept_name")
)
