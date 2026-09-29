from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc())
w2 = Window.partitionBy("dept_id").orderBy("salary", "id")
w3 = Window.orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_employees
    .withColumn("salary_dense_rank", F.dense_rank().over(w))
    .withColumn("salary_bucket", F.ntile(3).over(w2))
    .withColumn("running_salary", F.sum("salary").over(w3))
)
