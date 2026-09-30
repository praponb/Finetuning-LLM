from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("salary") <= 98000)
    .select("id", "dept_id", "hire_date", "level", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") <= 5)
)
