from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy(F.col("salary").desc())

result = (
    df_employees
    .select("id", "job_title", "salary", "full_name", F.rank().over(w).alias("pos"))
    .filter(F.col("pos") <= 2)
    .select("id", "job_title", "salary", "full_name")
)
