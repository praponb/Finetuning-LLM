from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("location").isin(["London", "Bangkok", "Singapore"]))
    .select("id", "dept_name", "budget", F.row_number().over(w).alias("rn"))
)
