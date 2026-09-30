from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy(F.col("salary").desc(), "id")

result = (
    df_employees
    .select("id", "level", "salary", "job_title", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 5)
    .select("id", "level", "salary", "job_title")
)
