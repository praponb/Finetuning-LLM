from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date", "id")

result = (
    df_employees
    .filter(F.col("salary") <= 106000)
    .select("id", "dept_id", "hire_date", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "dept_id", "hire_date")
    .orderBy(F.col("dept_id").desc())
)
