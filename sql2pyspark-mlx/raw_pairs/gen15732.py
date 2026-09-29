from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_name").orderBy(F.col("budget").desc(), "id")

result = (
    df_departments
    .filter(F.col("location").isNull())
    .select("id", "dept_name", "budget", F.row_number().over(w).alias("budget_row"))
)
