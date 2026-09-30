from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")
w2 = Window.partitionBy("manager_id", "job_title").orderBy("hire_date", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy(F.date_trunc("YEAR", "hire_date"), "level").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .filter(F.col("manager_id").isNotNull())
    .withColumn("next_salary", F.lead("salary", 2).over(w))
    .withColumn("salary_avg_5_prior", F.avg("salary").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)
