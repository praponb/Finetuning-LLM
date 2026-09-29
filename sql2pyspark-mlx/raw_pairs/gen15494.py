from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level", "dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .filter(F.col("full_name").isNotNull())
    .withColumn("first_known_job_title", F.first("job_title", ignorenulls=True).over(w))
    .withColumn("cumulative_salary", F.sum("salary").over(w2))
    .withColumn("cumulative_salary_2", F.sum("salary").over(w3))
)
