from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())

result = (
    df_departments
    .filter(F.col("location") != "Berlin")
    .select("id", "dept_name", "budget", "location", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 1)
    .orderBy("dept_name", "location")
)
