from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name", "location").orderBy(F.col("budget").desc(), "id")

base = (
    df_departments
    .filter(F.col("location").isin(["London", "New York", "Berlin"]))
    .withColumn("clean_dept_name", F.trim("dept_name"))
)

result = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "dept_name", "location", "budget", "clean_dept_name")
)
