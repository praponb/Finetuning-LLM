from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("manager_id").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("MONTH", "hire_date")).orderBy("hire_date", "id").rowsBetween(-6, -1)

result = (
    df_employees
    .withColumn("running_min_salary", F.min("salary").over(w))
    .withColumn("running_count_salary", F.count("salary").over(w))
    .withColumn("running_sum_salary", F.sum("salary").over(w))
    .withColumn("salary_avg_6_prior", F.avg("salary").over(w2))
)
