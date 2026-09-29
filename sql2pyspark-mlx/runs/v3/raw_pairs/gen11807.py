from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc())

result = (
    df_departments
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") <= 3)
    .select("id", "location", "budget", "dept_name")
)
