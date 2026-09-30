from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title").orderBy("salary", "id")

result = (
    df_employees
    .filter(F.col("level").isin(["L4", "L1"]))
    .select("id", "job_title", "salary", F.row_number().over(w).alias("row_num"))
    .filter(F.col("row_num") <= 3)
    .select("id", "job_title", "salary")
    .orderBy(F.col("id").desc())
)
