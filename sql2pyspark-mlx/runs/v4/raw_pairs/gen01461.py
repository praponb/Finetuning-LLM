from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .filter(F.col("budget") > 3174000)
    .select("id", "dept_name", "budget", "location", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .orderBy(F.col("rn").desc(), F.col("id").desc())
)
