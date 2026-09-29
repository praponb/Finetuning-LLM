from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("hire_date")

result = (
    df_employees
    .select("id", "dept_id", "hire_date", "manager_id", F.rank().over(w).alias("rn"))
    .filter(F.col("rn") <= 2)
    .select("id", "dept_id", "hire_date", "manager_id")
)
