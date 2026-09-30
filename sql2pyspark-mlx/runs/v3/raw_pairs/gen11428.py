from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy("budget", "id")

result = (
    df_departments
    .filter(F.col("budget").between(2471000, 2786000))
    .select("id", "dept_name", "budget", F.row_number().over(w).alias("row_num"))
    .orderBy("dept_name", F.col("budget").desc())
)
