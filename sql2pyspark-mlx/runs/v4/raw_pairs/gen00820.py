from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc())

result = (
    df_departments
    .withColumn("rnk", F.rank().over(w))
    .filter(F.col("rnk") <= 5)
    .select("id", "dept_name", "budget", "location")
)
