from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary")

result = (
    df_employees
    .filter(F.col("dept_id").isin([1, 13]))
    .select("id", "level", "salary", "job_title", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") <= 3)
    .orderBy("job_title")
)
