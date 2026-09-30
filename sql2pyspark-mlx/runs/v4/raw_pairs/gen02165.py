from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id")
w2 = Window.partitionBy("manager_id").orderBy("hire_date", "id")
w3 = Window.partitionBy("dept_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .withColumn("prev_salary", F.lag("salary", 1, 0).over(w))
    .withColumn("prev_salary_2", F.lag("salary", 2).over(w2))
    .withColumn("last_known_level", F.last("level", ignorenulls=True).over(w3))
)
