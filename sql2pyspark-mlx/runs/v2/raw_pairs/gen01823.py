from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")

result = (
    df_employees
    .withColumn("max_salary_by_dept", F.max("salary").over(w))
    .filter(F.col("salary") == F.col("max_salary_by_dept"))
    .select("id", "dept_id", "salary")
)
