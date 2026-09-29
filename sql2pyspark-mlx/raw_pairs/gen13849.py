from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("hire_date", "id")

result = (
    df_employees
    .select("id", "job_title", "hire_date", F.row_number().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .select("id", "job_title", "hire_date")
    .orderBy("hire_date", F.col("id").desc())
)
