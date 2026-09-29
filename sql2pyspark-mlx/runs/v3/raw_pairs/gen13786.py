from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")

result = (
    df_employees
    .withColumn("min_salary_by_dept", F.min("salary").over(w))
    .filter(F.col("salary") == F.col("min_salary_by_dept"))
    .select("id", "dept_id", "salary")
    .orderBy(F.col("salary").desc(), "id")
)
