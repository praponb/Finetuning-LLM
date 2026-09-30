from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date")

result = (
    df_employees
    .select("id", "job_title", "hire_date", F.rank().over(w).alias("hire_date_rank"))
    .orderBy(F.col("hire_date_rank").desc())
)
