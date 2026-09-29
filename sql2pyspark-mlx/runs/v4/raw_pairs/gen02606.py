from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("location").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .filter(F.col("dept_name") == "Finance")
    .select("id", "location", "budget", "dept_name", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") == 1)
    .select("id", "location", "budget", "dept_name")
)
