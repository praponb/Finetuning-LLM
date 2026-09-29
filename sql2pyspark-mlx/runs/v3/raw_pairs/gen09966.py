from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date")

result = (
    df_employees
    .filter(F.col("dept_id") == 25)
    .select("id", "job_title", "hire_date", F.rank().over(w).alias("rnk"))
)
