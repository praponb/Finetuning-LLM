from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("hire_date", "id").rowsBetween(-5, -1)
w2 = Window.orderBy(F.col("salary").desc(), "id")
w3 = Window.partitionBy("dept_id")

result = (
    df_employees
    .withColumn("salary_avg_5_prior", F.avg("salary").over(w))
    .withColumn("salary_bucket", F.ntile(3).over(w2))
    .withColumn("dept_id_max_salary", F.max("salary").over(w3))
)
