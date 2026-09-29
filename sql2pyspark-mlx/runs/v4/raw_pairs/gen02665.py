from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary")

deduped = (
    df_employees
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = deduped.select("id", "email", "job_title", "salary", "hire_date", "level")
