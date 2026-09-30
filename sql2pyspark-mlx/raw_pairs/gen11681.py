from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .filter(F.col("dept_id").isin([1, 13, 30]))
    .select("id", "job_title", "salary", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "job_title", "salary")
    .orderBy(F.col("job_title").desc())
)
