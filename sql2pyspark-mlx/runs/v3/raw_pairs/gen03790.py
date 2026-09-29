from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .select("id", "dept_name", "budget", "location", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
)
