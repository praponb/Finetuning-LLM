from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id")

result = (
    df_employees
    .withColumn("avg_salary_by_dept", F.avg("salary").over(w))
    .filter(F.col("salary") > F.col("avg_salary_by_dept"))
    .select("id", "dept_id", "salary")
)
