from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Engineering", "Sales"]))
    .select("id", "dept_name", "budget", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .select("id", "dept_name", "budget")
)
